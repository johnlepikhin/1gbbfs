open Common
open Dbt

module Config = struct
	let name = ref ""

	let client_row = ref (Client.default ())

	let fuse_args = ref []
end

let global_lock = Mutex.create ()

let in_single (f : unit -> 'a) : 'a =
		Mutex.lock global_lock;
		try
			let r = f () in
			Mutex.unlock global_lock;
			r
		with
			| e ->
				Mutex.unlock global_lock;
				raise e

let add_slash p =
	if p.[(String.length p) - 1] = '/' then p else p ^ "/"


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

let c_getattr path dbd =
	let r = Metadata.ro_of_path ~for_update:true ~use_cache:true dbd path in
	Lwt.return (Db.Data r.Metadata_S.stat)

let c_readdir path dbd =
	let open Metadata_S in
	let r = Metadata.ro_of_path ~for_update:true ~use_cache:true dbd path in
	match r.stat.Unix.LargeFile.st_kind with
		| Unix.S_DIR ->
			let fn = add_slash path in
			let rows = Metadata.ro_select_all dbd ("fullname like '" ^ (Db.like_escape fn) ^ "%' and deepness=" ^ (Mysql.ml2int (r.deepness + 1)) ^ " order by fullname") in
			let dlen = String.length fn in
			let names = List.map (fun row -> String.sub row.fullname dlen ((String.length row.fullname) - dlen)) rows in
			Lwt.return (Db.Data (List.sort compare names))
		| _ ->
			Lwt.return eNOTDIR

let c_mkdir path mode dbd =
	let parent_path = Filename.dirname path in
	let parent_path = if parent_path = "" then "/" else parent_path in
	let parent_meta = Metadata.rw_of_path dbd parent_path in

	let open Metadata_S in
	let row = Metadata.directory mode path in
	let row = { row with required_distribution = parent_meta.required_distribution; deepness = parent_meta.deepness + 1 } in
	let (_ : int64) = Metadata.insert dbd row in
	Lwt.return (Db.Data ())

let c_rmdir path dbd =
	let open Metadata_S in
	let r = Metadata.rw_of_path dbd path in
	match r.stat.Unix.LargeFile.st_kind with
		| Unix.S_DIR -> (
			(* use Mysql.escape to add unescaped percent sign *)
			let cnt = Db.RW.select_one dbd
				("select count(*) from " ^ Metadata.name ^ " where fullname like '" ^ (Db.like_escape r.fullname) ^ "/%' and deepness=" ^ (Mysql.ml2int r.deepness))
			in
			match cnt.(0) with
				| None ->
					Lwt.return eINVAL
				| Some "0" ->
					let success = ref 0 in
					let f ~backend ~reg_backend ~row conn =
						let open T_communication in
						lwt r = Connection.request conn (Cmd.RmDir reg_backend.RegBackend_S.path) in
						match r with
							| Response.Data ()
							| Response.UnixError Unix.ENOENT ->
								RegBackend.delete dbd reg_backend;
								incr success;
								Lwt.return ()
							| Response.UnixError e ->
								debug "Got Unix error: %s" (Unix.error_message e);
								let reg_backend = { reg_backend with RegBackend_S.metadata_update_tries = reg_backend.RegBackend_S.metadata_update_tries + 1 } in
								RegBackend.update_by_key dbd reg_backend;
								Lwt.return ()
							| Response.Error s ->
								debug "Got error: %s" s;
								let reg_backend = { reg_backend with RegBackend_S.metadata_update_tries = reg_backend.RegBackend_S.metadata_update_tries + 1 } in
								RegBackend.update_by_key dbd reg_backend;
								Lwt.return ()
					in
					let backends = RegBackend.rw_all_of_metadata dbd r in
					lwt () = Connection_pool.do_for_all_p ~getter:(fun b -> b) ~f backends in

					if !success = List.length backends then
						Metadata.delete dbd r
					else
						Metadata.update_where dbd { r with fullname = "" } ("id=" ^ (Mysql.ml642int r.id));
					Lwt.return (Db.Data ())
				| Some _ ->
					Lwt.return (Db.Failure Unix.ENOTEMPTY)
		)
		| _ ->
			Lwt.return eNOTDIR

let c_unlink path dbd =
	let r = Metadata.rw_of_path dbd path in
(*
	protect_open_file dbd r (fun r ->
*)
		let open Metadata_S in
		match r.stat.Unix.LargeFile.st_kind with
			| Unix.S_REG -> (
				let f ~backend ~reg_backend ~row conn =
					let delete () =
						RegBackend.delete dbd row;
						Lwt.return ()
					in
					match row.RegBackend_S.path with
						| "" ->
							delete ()
						| bpath ->
							lwt r = Connection.request conn (T_communication.Cmd.Unlink bpath) in
							let open T_communication.Response in
							let open RegBackend_S in
							match r with
								| Data () ->
									delete ();
								| Error _ ->
									RegBackend.update_where dbd { row with state = Deleted; metadata = None } ("id=" ^ (Mysql.ml642int row.id));
									Lwt.return ()
								| _ ->
									let row = { row with state = Immutable; metadata = None; metadata_update_tries = row.metadata_update_tries + 1; } in
									RegBackend.update_where dbd row ("id=" ^ (Mysql.ml642int row.id));
									Lwt.return ()
				in
				let on_error ~reg_backend ~row exn =
					let open RegBackend_S in
					RegBackend.update_where dbd { row with state = Deleted; metadata = None } ("id=" ^ (Mysql.ml642int row.id));
					Lwt.return ()
				in
				let backends = RegBackend.rw_all_of_metadata dbd r in
				lwt () = Connection_pool.do_for_all_p ~on_error ~getter:(fun b -> b) ~f backends in
				Metadata.delete dbd r;
				Lwt.return (Db.Data ())
			)
			| _ ->
				Lwt.return eINVAL
(*
	)
*)

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
									Lwt.return ()
								| Response.UnixError e ->
									debug "Remote server Unix error: %s" (Unix.error_message e);
									Lwt.return ()
								| Response.Error e ->
									debug "Remote server error: %s" e;
									Lwt.return ()
						in
						let backends = RegBackend.rw_all_of_metadata dbd r in
						lwt () = Connection_pool.do_for_all_p ~getter:(fun b -> b) ~f backends in
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
		lwt () = Connection_pool.do_for_all_s ~on_error ~getter:(fun b -> b.Fd_client.regbackend) ~f backends in
		match !read_bytes with
			| Some rb ->
				Lwt.return (Db.Data rb)
			| None ->
				Lwt.return eAGAIN
	with
		| Not_found ->
			Lwt.return eBADF

let c_write path buffer offset size dbd =
	let size = Bigarray.Array1.dim buffer in
	try
		let fd = Fd_client.get path in
		let open T_communication in
		let saved_counter = ref 0 in
		let f ~backend ~reg_backend ~row conn =
			lwt () =
				if row.Fd_client.regbackend.RegBackend_S.state = RegBackend_S.Valid then
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
		let backends = Fd_client.get_all_backends fd in
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

let get_parent_meta path dbd =
	let parent_path = Filename.dirname path in
	let parent_path = if parent_path = "" then "/" else parent_path in
	Metadata.rw_of_path dbd parent_path

let generate_prefix () =
	Printf.sprintf "__bfs_renamed_%.0f_%i__" (Unix.gettimeofday ()) (Random.int 1000000)

let rec backend_mkdir ?(prefix="") dirname parent_path metadata backend_id dbd =
	let parent_path = add_slash parent_path in
	let try_other_name () =
		let prefix = generate_prefix () in
		backend_mkdir ~prefix dirname parent_path metadata backend_id dbd
	in
	let fullpath = parent_path ^ prefix ^ dirname in
	try_lwt
		(* check if element with such name already exists on backend *)
		let (_ : RegBackend_S.row) = RegBackend.rw_of_backend_and_path ~backend:backend_id ~path:fullpath dbd in
		try_other_name ()
	with
		| Not_found ->
			let r = ref None in
			let open T_communication in
			let f c =
				lwt res = Connection.request c (Cmd.MkDir fullpath) in
				r := res;
				Lwt.return ()
			in
			let backend_srv = Backend.of_backend_id backend_id in
			lwt () = Connection_pool.wrap ~f backend_srv.T_server.addr in
			match !r with
				| Some R_MkDir.OK ->
					let reg = RegBackend.create ~metadata:(Some metadata.Metadata_S.id) ~backend:backend_id fullpath in
					let id = RegBackend.insert dbd reg in
					Lwt.return { reg with RegBackend_S.id = id }
				| Some R_MkDir.AlreadyExists ->
					try_other_name ()
				| None ->
					debug "Call to MkDir on backend wasn't successful";
					Lwt.fail Connection_pool.NotAvailable

let rec backend_checkdir metadata backend_id dbd =
	if metadata.Metadata_S.fullname = "/" then
		Lwt.return "/"
	else
		lwt reg_backend =
			try
				let r = RegBackend.rw_of_metadata_and_backend ~metadata ~backend:backend_id dbd in
				(* directory exists, just return *)
				Lwt.return r
			with
				| Not_found ->
					(* directory have to be created *)
					let parent_meta = get_parent_meta (metadata.Metadata_S.fullname) dbd in
					lwt parent_path =
						if parent_meta.Metadata_S.fullname = "/" then
							Lwt.return "/"
							(* OK, root always exist *)
						else
						begin
							(* parent isn't root, check it recursively *)
							backend_checkdir parent_meta backend_id dbd
							(* all parents exist, it's time to create directory and return *)
						end
					in
					let dirname = Filename.basename metadata.Metadata_S.fullname in
					backend_mkdir dirname parent_path metadata backend_id dbd
		in
		Lwt.return reg_backend.RegBackend_S.path

let c_create path mode dbd =
	let parent_meta = get_parent_meta path dbd in

	let open Metadata_S in
	let meta = Metadata.regular mode path in
	let meta = { meta with deepness = parent_meta.deepness + 1; required_distribution = parent_meta.required_distribution } in
	let id = Metadata.insert dbd meta in
	let backends = Backend.get_best_for_create parent_meta.required_distribution in
	lwt () = Lwt_list.iter_p (fun backend ->
		lwt parent_backend_path = backend_checkdir parent_meta backend.T_server.id dbd in
		let ppath = add_slash parent_backend_path in
		let basename = Filename.basename path in
		let rec try_prefix prefix =
			try
				let backend_path = ppath ^ prefix ^ basename in
				let regbackend =  RegBackend.create ~metadata:(Some id) ~backend:backend.T_server.id backend_path in
				let (_ : int64) = RegBackend.insert dbd regbackend in
				Lwt.return ()
			with
				| _ ->
					let prefix = generate_prefix () in
					try_prefix prefix
		in
		try_prefix ""
	) backends in
	c_fopen path [Unix.O_WRONLY; Unix.O_CREAT] dbd

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

let c_flush path fh dbd =
	try
		Lwt.return (Db.Data ())
	with
		| Not_found ->
			Lwt.return eBADF

let c_utime path atime mtime dbd =
	let r = Metadata.rw_of_path dbd path in
	let open Metadata_S in
	Metadata.update_by_key dbd { r with stat = { r.stat with Unix.LargeFile.st_mtime = mtime } };
	Lwt.return (Db.Data ())

let c_chmod path mode dbd =
	let r = Metadata.rw_of_path dbd path in
	let open Metadata_S in
	Metadata.update_by_key dbd { r with stat = { r.stat with Unix.LargeFile.st_perm = mode } };
	Lwt.return (Db.Data ())

let c_chown path st_uid st_gid dbd =
	let r = Metadata.rw_of_path dbd path in
	let open Metadata_S in
	let open Unix.LargeFile in
	Metadata.update_by_key dbd { r with stat = { r.stat with st_uid; st_gid } };
	Lwt.return (Db.Data ())

let c_truncate path len dbd =
	try
		let fd = Fd_client.get path in
		let open T_communication in
		let saved_counter = ref 0 in
		let f ~backend ~reg_backend ~row conn =
			lwt () =
				if row.Fd_client.regbackend.RegBackend_S.state = RegBackend_S.Valid then
					match row.Fd_client.remote_fd with
						| Some remote_fd -> (
							lwt r = Connection.request conn (Cmd.Truncate (remote_fd, len)) in
							match r with
								| Response.Data () ->
									incr saved_counter;
									Lwt.return ()
								| Response.UnixError e ->
									debug "Remote server Unix error: %s" (Unix.error_message e);
									RegBackend.mark_invalid ~from_pos:len reg_backend;
									Lwt.return ()
								| Response.Error e ->
									debug "Remote server error: %s" e;
									RegBackend.mark_invalid ~from_pos:len reg_backend;
									Lwt.return ()
						)
						| None ->
							RegBackend.mark_invalid ~from_pos:len reg_backend;
							Lwt.return ()
				else
				begin
					RegBackend.mark_invalid ~from_pos:len reg_backend;
					Lwt.return ()
				end
			in
			Lwt.return true
		in
		let on_error ~reg_backend ~row exn =
			RegBackend.mark_invalid ~from_pos:len reg_backend;
			Lwt.return true (* continue *)
		in
		let backends = Fd_client.get_valid_backends fd in
		debug "selected %i backends" (List.length backends);
		lwt () = Connection_pool.do_for_all_s ~on_error ~getter:(fun b -> b.Fd_client.regbackend) ~f backends in
		if !saved_counter = 0 then
		begin
			debug "truncate failed, written to zero backends";
			Lwt.return eAGAIN
		end
		else
		begin
			Metadata.shrink ~size:len fd.Fd_client.metadata;
			Lwt.return (Db.Data ())
		end
	with
		| Not_found ->
			Lwt.return eBADF


let c_rename oname nname dbd =
	let open Metadata_S in
	let r = Metadata.rw_of_path dbd oname in
	let parent_meta = get_parent_meta nname dbd in
	let success = ref 0 in
	let f ~backend ~reg_backend ~row conn =
		lwt parent_backend_path = backend_checkdir parent_meta backend.T_server.id dbd in
		let ppath = add_slash parent_backend_path in
		let basename = Filename.basename nname in
		let backend_path_old = reg_backend.RegBackend_S.path in
		let rec try_prefix prefix =
			let try_other_prefix () =
				let prefix = generate_prefix () in
				try_prefix prefix
			in
			let open T_communication in
			let backend_path_new = ppath ^ prefix ^ basename in
			lwt r = Connection.request conn (Cmd.Rename (backend_path_old, backend_path_new)) in
			match r with
				| Response.Data () ->
					(try
						Dbt.RegBackend.delete_where dbd
							("path=" ^ (Mysql.ml2str backend_path_new) ^ " and backend=" ^ (Mysql.ml642int reg_backend.RegBackend_S.backend))
					with _ -> ());
					Dbt.RegBackend.update_by_key dbd { reg_backend with RegBackend_S.path = backend_path_new };
					(* rename all childs on the backend *)
					let old_with_slash = add_slash backend_path_old in
					let new_with_slash = add_slash backend_path_new in
					let old_length = String.length old_with_slash in
					lwt () = Db.RW.iter_all dbd
						("select " ^ RegBackend.cols_string ^ " from " ^ RegBackend.name
						^ " where path like '" ^ (Db.like_escape old_with_slash) ^ "%' and backend=" ^ (Mysql.ml642int reg_backend.RegBackend_S.backend) ^ " for update")
						(fun row ->
							let row = RegBackend_S.of_db row in
							let tail = String.sub row.RegBackend_S.path old_length ((String.length row.RegBackend_S.path) - old_length) in
							let path = new_with_slash ^ tail in
							RegBackend.update_by_key ~lock:false dbd { row with RegBackend_S.path = path };
							Lwt.return ()
						)
					in
					incr success;
					Lwt.return ()
				| Response.UnixError Unix.EXDEV ->
					(* TODO schedule recursive rename *)
					Lwt.return ()
				| Response.UnixError Unix.EEXIST ->
					try_other_prefix ()
				| Response.UnixError _
				| Response.Error _ ->
					Lwt.return ()
		in
		try_prefix ""
	in
	let backends = RegBackend.rw_all_of_metadata dbd r in
	lwt () = Connection_pool.do_for_all_p ~getter:(fun b -> b) ~f backends in

	let nwhere = ("fullname=" ^ (Mysql.ml2str nname)) in
	if !success = List.length backends then
		Metadata.delete_where dbd nwhere
	else
	begin
		let m = Metadata.rw_of_path dbd nname in
		Metadata.update_where dbd { m with fullname = "" } nwhere
	end;

	(* rename all childs *)
	let old_with_slash = add_slash oname in
	let new_with_slash = add_slash nname in
	let old_length = String.length old_with_slash in
	lwt () = Db.RW.iter_all dbd
		("select " ^ Metadata.cols_string ^ " from " ^ Metadata.name ^ " where fullname like '" ^ (Db.like_escape old_with_slash) ^ "%' for update")
		(fun row ->
			let row = of_db row in
			let tail = String.sub row.fullname old_length ((String.length row.fullname) - old_length) in
			let fullname = new_with_slash ^ tail in
			let deepness = ref 0 in
			String.iter (fun c -> if c = '/' then incr deepness) fullname;
			Metadata.update_where ~lock:false dbd { row with fullname; deepness = !deepness } ("fullname=" ^ (Mysql.ml2str row.fullname));
			Lwt.return ()
		)
	in

	(* finalize *)
	Metadata.update_where dbd { r with fullname = nname; deepness = parent_meta.deepness + 1 } ("id=" ^ (Mysql.ml642int r.id));
	Lwt.return (Db.Data ())

let rec update_backends_periodically pool my_id =
	Db.ro_wrap pool "t_update_backends" (fun dbd -> Backend.update_backends dbd my_id; Lwt.return (Db.Data ()));
	Unix.sleep Common.backends_update_period;
	update_backends_periodically pool my_id

let rec gc_periodically () =
	Gc.compact ();
	Unix.sleep 10;
	gc_periodically ()


let show_clients () =
	let pool = Db_pool.create !Common.mysql_config in
	Db.ro_wrap pool "show_clients" (fun dbd ->
		let rows = Client.ro_select_all dbd "1" in
		List.iter (fun c -> Printf.printf "name=%s\n" c.Client_S.name) rows;
		exit 0
	)

let args = [
	"--name", Arg.Set_string Config.name, "Client name (as in 'client' table)";
	"--show-all", Arg.Unit show_clients, "Show all clients";
	"--mysql-host", Arg.String (fun v -> Common.mysql_config := { !Common.mysql_config with Mysql.dbhost = Some v}), "Set MySQL host";
	"--mysql-port", Arg.Int (fun v -> Common.mysql_config := { !Common.mysql_config with Mysql.dbport = Some v}), "Set MySQL port";
	"--mysql-user", Arg.String (fun v -> Common.mysql_config := { !Common.mysql_config with Mysql.dbuser = Some v}), "Set MySQL user name";
	"--mysql-db", Arg.String (fun v -> Common.mysql_config := { !Common.mysql_config with Mysql.dbname = Some v}), "Set MySQL database name";
	"--", Arg.Rest (fun arg -> Config.fuse_args := arg :: !Config.fuse_args), "FUSE arguments"
]

let args_usage = "Usage:\n\nbfs_client --name CLIENT_NAME -- [fuse options ..]\n"



let () =
	Arg.parse args (fun s -> raise (Arg.Bad s)) args_usage;
	if !Config.name = "" then (
		Printf.eprintf "%s" (Arg.usage_string args args_usage);
		exit 1
	);

	Random.self_init ();
	let open Fuse in
	let pool = Db_pool.create !Common.mysql_config in
(*
	let null_wrap name f = Db.null_wrap pool name f in
*)
	let ro_wrap name f = Db.ro_wrap pool name f in
	let rw_wrap name f = Db.rw_wrap pool name f in

	ro_wrap "get client info" (fun dbd ->
		try
			let row = Client.ro_of_name dbd !Config.name in
			Config.client_row := row;
			Lwt.return (Db.Data ())
		with
			| Not_found ->
				Printf.eprintf "Client '%s' not found in DB\n" !Config.name;
				exit 1
	);

	let (_ : Thread.t) = Thread.create (Db_pool.cleanup_periodically pool) 3 in
	let (_ : Thread.t) = Thread.create (update_backends_periodically pool) !Config.client_row.Client_S.id in
	let (_ : Thread.t) = Thread.create Dbt.periodic_processor () in
	let (_ : Thread.t) = Thread.create gc_periodically () in
	let fuse_args = Array.of_list (Sys.argv.( 0 ) :: List.rev !Config.fuse_args) in
	let fTODO = fun _ -> Lwt.return eTODO in
	main fuse_args { default_operations with
		getattr = (fun path -> in_single (fun () -> ro_wrap "getattr" (c_getattr path)));
		readlink = (fun path -> ro_wrap "readlink" fTODO);
		(* mknod *)
		mkdir = (fun path mode -> in_single (fun () -> rw_wrap "mkdir" (c_mkdir path mode)));
		unlink = (fun path -> in_single (fun () -> rw_wrap "unlink" (c_unlink path)));
		rmdir = (fun path -> in_single (fun () -> rw_wrap "rmdir" (c_rmdir path)));
(*		symlink = (fun path_dest path_link -> rw_wrap "symlink" (c_symlink path_dest path_link)); *)

		rename = (fun oname nname -> in_single (fun () -> rw_wrap "rename" (c_rename oname nname)));

		link = (fun path_dest path_link -> rw_wrap "link" fTODO);
		chmod = (fun path mode -> in_single (fun () -> rw_wrap "chmod" (c_chmod path mode)));
		chown = (fun path uid gid -> in_single (fun () -> rw_wrap "chown" (c_chown path uid gid)));
		truncate = (fun path len -> in_single (fun () -> rw_wrap "truncate" (c_truncate path len)));
		utime = (fun path atime mtime -> in_single (fun () -> rw_wrap "utime" (c_utime path atime mtime)));
		fopen = (fun path flags -> in_single (fun () -> rw_wrap "fopen" (c_fopen path flags)));
		read = (fun path buffer offset size -> in_single (fun () -> let r = ro_wrap "read" (c_read path buffer offset size) in debug "------- %i" r; r));
		write = (fun path buffer offset size -> ro_wrap "write" (c_write path buffer offset size));
		statfs = (fun path -> rw_wrap "statfs" fTODO);
		flush = (fun path fh -> in_single (fun () -> rw_wrap "flush" (c_flush path fh)));
		release = (fun path flags fh -> in_single (fun () -> rw_wrap "release" (c_release path flags fh)));
		(* fsync *)
		(* setxattr *)
		(* getxattr *)
		(* listxattr *)
		(* removexattr *)
		opendir = (fun _ _ -> None);
		readdir = (fun dir _ -> in_single (fun () -> ro_wrap "readdir" (c_readdir dir)));
		releasedir = (fun _ _ _ -> ());
		(* fsyncdir *)
		(* init *)
		create = (fun path mode -> in_single (fun () -> rw_wrap "create" (c_create path mode)));
	}
