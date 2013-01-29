open Dbt
open Common

let c_release path flags fh dbd =
	debug :1 "close %s" path;
	try
		let last_fd = Fd_client.close_file path in
		lwt () =
			match last_fd with
				| Some fd ->
					let f ~backend ~(reg_backend : Dbt.RegBackend.row) ~(row : Fd_client.backend) conn =
						match row.Fd_client.remote_fd with
							| Some remote_fd -> (
								let open T_communication in
								lwt r = Connection.request conn (Cmd.FClose remote_fd) in
								match r with
									| Response.Data () ->
										Lwt.return ()
									| Response.UnixError e ->
										debug "Remote server Unix error: %s" (Unix.error_message e);
										Lwt.return ()
									| Response.Error e ->
										debug "Remote server error: %s" e;
										Lwt.return ()
							)
							| None ->
								Lwt.return ()
					in
					let on_error ~reg_backend ~row exn = Lwt.return () in
					let (backends : Fd_client.backend list) = Fd_client.get_all_backends fd in
					lwt () = Connection_pool.do_for_all_p ~on_error ~getter:(fun b -> b.Fd_client.regbackend) ~f backends in
					Fd_client.db_sync dbd fd;
					Fd_client.remove path;
					Lwt.return ()
				| None ->
					Lwt.return ()
		in
		Mutex.unlock Fd_client.global_lock;
		Lwt.return (Db.Data ())
	with
		| Not_found ->
			Mutex.unlock Fd_client.global_lock;
			Lwt.return eBADF


