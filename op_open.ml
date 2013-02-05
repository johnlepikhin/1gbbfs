open Dbt
open Common

let protect_open_file dbd meta f =
	let rec loop meta attempt =
		if meta.Metadata_S.fh_counter > 0 then
		begin
			if attempt > 10 then
			begin
				debug "File marked as open for %i secs, fail" attempt;
				Lwt.return ePERM
			end
			else
			begin
				debug "File %s marked as open, wait..." meta.Metadata_S.fullname;
				lwt () = Lwt_unix.sleep 1. in
				let newmeta = Metadata.rw_of_path ~use_cache:false dbd meta.Metadata_S.fullname in
				loop newmeta (attempt + 1)
			end
		end
		else
			f meta
			
	in
	loop meta 1

let c_fopen path flags dbd =
	if Fd_client.soft_open_file path then
		Lwt.return (Db.Data None)
	else (
		let flags = List.filter ((<>) Unix.O_WRONLY) flags in
		let flags = List.filter ((<>) Unix.O_RDONLY) flags in
		let flags = List.filter ((<>) Unix.O_RDWR) flags in
		let flags = Unix.O_RDWR :: flags in
		let open Metadata_S in
		let r = Metadata.rw_of_path dbd path in
		match r.stat.Unix.LargeFile.st_kind with
			| Unix.S_DIR ->
				Lwt.return eISDIR
			| Unix.S_REG -> (
				let fd = Fd_client.open_file { r with fh_counter = r.fh_counter + 1 } in
				if fd.Fd_client.backends = [] then (
					(* is new, need to open connections *)
					(* is file opened on another server? *)
					protect_open_file dbd r (fun r ->
	(*
						let r = { r with fh_counter = r.fh_counter + 1 } in
						let fd = Fd_client.open_file r in
	*)
						let r = { r with fh_counter = r.fh_counter + 1 } in
						let f ~backend ~reg_backend ~row conn =
							let open T_communication in
							lwt r = Connection.request conn (Cmd.FOpen (reg_backend.RegBackend_S.path, flags)) in
							match r with
								| Response.Data server_fd ->
									debug "File %s opened, received remote FD=%Li" path server_fd;
									Fd_client.add_backend fd reg_backend backend (Some server_fd);
									debug "added OK";
									Lwt.return ()
								| Response.UnixError Unix.ENOENT ->
									debug "Remote server Unix error: %s" (Unix.error_message Unix.ENOENT);
									Fd_client.add_backend fd reg_backend backend None;
									Lwt.return ()
								| Response.UnixError e ->
									Fd_client.add_backend fd reg_backend backend None;
									debug "Remote server Unix error: %s" (Unix.error_message e);
									Lwt.return ()
								| Response.Error e ->
									Fd_client.add_backend fd reg_backend backend None;
									debug "Remote server error: %s" e;
									Lwt.return ()
						in
						let backends = RegBackend.rw_all_of_metadata dbd r in
						let on_error ~reg_backend ~row _ =
							let backend = Backend.of_reg_backend reg_backend in
							Fd_client.add_backend fd reg_backend backend None;
							Lwt.return ()
						in
						lwt () = Connection_pool.do_for_all_p ~on_error ~getter:(fun b -> b) ~f backends in
						match fd.Fd_client.backends with
							| [] ->
								Fd_client.unsafe_remove fd.Fd_client.metadata.Metadata_S.fullname;
								Mutex.unlock fd.Fd_client.mutex;
								Lwt.return eAGAIN
							| _ ->
								Metadata.update_by_key dbd r;
								Mutex.unlock fd.Fd_client.mutex;
								Lwt.return (Db.Data None)
					)
				) else (
					(* file is already opened here, just unlock fd.mutex *)
					Mutex.unlock fd.Fd_client.mutex;
					Lwt.return (Db.Data None)
				)
			)
			| _ ->
				Lwt.return eINVAL
	)


