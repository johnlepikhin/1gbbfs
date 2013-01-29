open Dbt
open Common

let c_read path buffer offset size dbd =
	let size = Bigarray.Array1.dim buffer in
	try
		let open T_communication in
		let read_bytes = ref None in
		let fd = Fd_client.get path in
		let f ~backend ~reg_backend ~row conn =
			match row.Fd_client.remote_fd with
				| Some remote_fd -> (
					lwt r = Connection.request conn (Cmd.Read (remote_fd, offset, size)) in
					match r with
						| Response.Data len ->
							lwt () = Connection.read_to_bigarray conn buffer 0 len in
							read_bytes := Some len;
							Lwt.return false (* OK, do not continue *)
						| Response.UnixError e ->
							Lwt.return true
						| Response.Error e ->
							Lwt.return true
				)
				| None ->
					Lwt.return true (* no FD, try next server *)
		in
		let on_error ~reg_backend ~row exn =
			Lwt.return true (* continue *)
		in
		let backends = Fd_client.get_valid_backends fd in
		let backends = List.sort (fun a b -> compare b.Fd_client.server.T_server.db_prio_read a.Fd_client.server.T_server.db_prio_read) backends in
		lwt () = Connection_pool.do_for_all_s ~on_error ~getter:(fun b -> b.Fd_client.regbackend) ~f backends in
		match !read_bytes with
			| Some rb ->
				Lwt.return (Db.Data rb)
			| None ->
				Lwt.return eAGAIN
	with
		| Not_found ->
			Lwt.return eBADF


