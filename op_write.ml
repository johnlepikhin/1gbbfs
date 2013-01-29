open Dbt
open Common

type do_on =
	| All
	| Valid
	| Invalid

let c_write ?(do_on=All) path buffer offset size dbd =
	let size = Bigarray.Array1.dim buffer in
	try
		let fd = Fd_client.get path in
		let open T_communication in
		let saved_counter = ref 0 in
		let f ~backend ~reg_backend ~row conn =
			lwt () =
				if row.Fd_client.regbackend.RegBackend_S.state = RegBackend_S.Valid || row.Fd_client.regbackend.RegBackend_S.max_valid_pos >= offset then
					match row.Fd_client.remote_fd with
						| Some remote_fd -> (
							lwt () = Connection.send_value conn (Cmd.Write (remote_fd, offset, size)) in
							lwt () = Connection.write_from_bigarray conn buffer 0 size in
							lwt r = Connection.read_value conn in
							match r with
								| Response.Data () ->
									incr saved_counter;
									let max_pos = Int64.add offset (Int64.of_int size) in
									if reg_backend.RegBackend_S.max_valid_pos < max_pos then
										reg_backend.RegBackend_S.max_valid_pos <- max_pos;
									Lwt.return ()
								| Response.UnixError e ->
									debug "Remote server Unix error: %s" (Unix.error_message e);
									RegBackend.mark_invalid ~from_pos:offset reg_backend;
									Lwt.return ()
								| Response.Error e ->
									debug "Remote server error: %s" e;
									RegBackend.mark_invalid ~from_pos:offset reg_backend;
									Lwt.return ()
						)
						| None ->
							RegBackend.mark_invalid ~from_pos:offset reg_backend;
							Lwt.return ()

				else
				begin
					RegBackend.mark_invalid ~from_pos:offset reg_backend;
					Lwt.return ()
				end
			in
			Lwt.return true
		in
		let on_error ~reg_backend ~row exn =
			RegBackend.mark_invalid ~from_pos:offset reg_backend;
			Lwt.return true (* continue *)
		in
		let backends = match do_on with
			| All -> Fd_client.get_all_backends fd
			| Valid -> Fd_client.get_valid_backends fd
			| Invalid -> Fd_client.get_invalid_backends fd
		in
		debug "selected %i backends" (List.length backends);
		lwt () = Connection_pool.do_for_all_s ~on_error ~getter:(fun b -> b.Fd_client.regbackend) ~f backends in
		if !saved_counter = 0 then
		begin
			debug "write failed, written to zero backends";
			Lwt.return eAGAIN
		end
		else
		begin
			Metadata.grow ~size:(Int64.add offset (Int64.of_int size)) fd.Fd_client.metadata;
			Lwt.return (Db.Data size)
		end
	with
		| Not_found ->
			Lwt.return eBADF


