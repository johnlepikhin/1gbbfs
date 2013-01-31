
open T_server
open T_communication

module FD = struct
	type t = {
		fd : Unix.file_descr;
		id : int64;
		path : string;
		mutable refs : int;
		fd_mutex : Mutex.t;
	}

	let mutex = Mutex.create ()

	let fds = Hashtbl.create 1003

	let fd_names = Hashtbl.create 1003

	let gen_id =
		let i = ref 0L in
		fun () ->
			i := Int64.add !i 1L;
			!i

	let create fd path = {
		fd;
		path;
		id = gen_id ();
		refs = 1;
		fd_mutex = Mutex.create ();
	}

	let add path fd =
		Mutex.lock mutex;
		try
			let id = Hashtbl.find fd_names path in
			let t = Hashtbl.find fds id in
			t.refs <- t.refs + 1;
			Mutex.unlock mutex;
			t.id
		with
			| _ ->
				let t = create fd path in
				Hashtbl.add fd_names path t.id;
				Hashtbl.add fds t.id t;
				Mutex.unlock mutex;
				t.id

	let remove id =
		Mutex.lock mutex;
		try
			let t = Hashtbl.find fds id in
			let is_last =
				if t.refs > 1 then (
					t.refs <- t.refs - 1;
					None
				) else (
					Hashtbl.remove fds t.id;
					Hashtbl.remove fd_names t.path;
					Some t.fd
				)
			in
			Mutex.unlock mutex;
			is_last
		with
			| _ ->
				Mutex.unlock mutex;
				None

	let get_by_id id =
		Hashtbl.find fds id

	let id_of_name path =
		Hashtbl.find fd_names path

	let lock t =
		Mutex.lock t.fd_mutex

	let unlock t =
		Mutex.unlock t.fd_mutex
end

type info = {
	connection : T_connection.t;
	backend : T_server.server;
	fds : (int64, unit) Hashtbl.t;
}

module Config = struct
	let name = ref ""

	let foreground = ref false
end

let fatal s =
	Printf.eprintf "Fatal: %s\n" s;
	exit 1

let c_fopen path flags c =
	let id =
		try
			FD.id_of_name path
		with
			| Not_found ->
				let fullpath = c.backend.T_server.storage ^ path in
				let fd = Unix.openfile fullpath flags 0o644 in
				let id = FD.add path fd in
				debug "send fd ID=%Li" id;
				id
	in
	Hashtbl.replace c.fds id ();
	Connection.send_value c.connection (Response.Data id)

let c_read fd offset size c =
	try
		let fd = FD.get_by_id fd in
		let buf = String.create size in
		try
			let rec loop p tl =
				match tl with
					| 0 -> 0
					| tl ->
						FD.lock fd;
						ignore (Unix.LargeFile.lseek fd.FD.fd (Int64.add offset (Int64.of_int p)) Unix.SEEK_SET);
						let sz = Unix.read fd.FD.fd buf p tl in
						FD.unlock fd;
						if sz = 0 then
							tl
						else
							loop (p+sz) (tl-sz)
			in
			let tl = loop 0 size in
			let rb = size - tl in
			debug "send read size %i" rb;
			lwt () = Connection.send_value c.connection (Response.Data rb) in
			Connection.send_string c.connection buf 0 rb
		with
			| End_of_file ->
				Connection.send_value c.connection (Response.Data 0);
	with
		| Not_found ->
			Connection.send_value c.connection (Response.UnixError Unix.EBADF)

let c_write fd offset size c =
	let rec loop fd buf p = function
		| 0 -> ()
		| tl ->
			FD.lock fd;
			ignore (Unix.LargeFile.lseek fd.FD.fd offset Unix.SEEK_SET);
			let wb = Unix.write fd.FD.fd buf p tl in
			FD.unlock fd;
			loop fd buf (p+wb) (tl-wb)
	in
	try
		lwt buf = Connection.read_string c.connection size in
		let fd = FD.get_by_id fd in
		loop fd buf 0 size;
		Connection.send_value c.connection (Response.Data ());
	with
		| Not_found ->
			debug "FD with ID=%Li not found" fd;
			Connection.send_value c.connection (Response.UnixError Unix.EBADF)
		| Unix.Unix_error (e, _, _) ->
			debug "Other unix error for ID=%Li" fd;
			Connection.send_value c.connection (Response.UnixError e)

let c_unlink path c =
	let () =
		try
			let fullpath = c.backend.T_server.storage ^ path in
			Unix.unlink fullpath
		with
			| Not_found ->
				()
	in
	Connection.send_value c.connection (Response.Data ())

let c_truncate fd len c =
	try
		let fd = FD.get_by_id fd in
		Unix.LargeFile.ftruncate fd.FD.fd len;
		Connection.send_value c.connection (Response.Data ());
	with
		| Not_found ->
			Connection.send_value c.connection (Response.UnixError Unix.EBADF)
		| Unix.Unix_error (e, _, _) ->
			Connection.send_value c.connection (Response.UnixError e)

let c_mkdir path c =
	try
		let fullpath = c.backend.T_server.storage ^ path in
		Unix.mkdir fullpath 0o755;
		Connection.send_value c.connection (Response.Data R_MkDir.OK)
	with
		| Unix.Unix_error (Unix.EEXIST, _, _) ->
			Connection.send_value c.connection (Response.Data R_MkDir.AlreadyExists)

let c_rmdir path c =
	let fullpath = c.backend.T_server.storage ^ path in
	Unix.rmdir fullpath;
	Connection.send_value c.connection (Response.Data ())

let close_fd fd =
	let fd = FD.remove fd in
	match fd with
		| None -> ()
		| Some fd ->
			Unix.close fd
	

let c_fclose fd c =
	close_fd fd;
	Hashtbl.remove c.fds fd;
	Connection.send_value c.connection (Response.Data ())

let c_rename oname nname c =
	Unix.rename (c.backend.T_server.storage ^ oname) (c.backend.T_server.storage ^ nname);
	Connection.send_value c.connection (Response.Data ())

let resp_wrap conn f =
	try
		f conn
	with
		| Unix.Unix_error (e, _, _) ->
			Connection.send_value conn.connection (Response.UnixError e)
		| e ->
			Connection.send_value conn.connection (Response.Error (Printexc.to_string e))

let drop_connection c =
	Hashtbl.iter (fun id _ -> close_fd id) c.fds;
	Connection.disconnect c.connection

let rec processor c =
	debug "Waiting for a command";
	debug "names=%i ids=%i" (Hashtbl.length FD.fd_names) (Hashtbl.length FD.fds);
	lwt cmd = Connection.read_value ~timeout:864000. c.connection in
	match cmd with
		| Cmd.Bye ->
			debug "Got Bye command";
			drop_connection c
		| Cmd.FOpen (path, flags) ->
			debug "Got FOpen '%s' command" path;
			lwt () = resp_wrap c (c_fopen path flags) in
			processor c
		| Cmd.Read (fd, offset, size) ->
			debug "Got Read (%Li, %Li, %i) command" fd offset size;
			lwt () = resp_wrap c (c_read fd offset size) in
			processor c
		| Cmd.Write (fd, offset, size) ->
			debug "Got Write (%Li, %Li, %i) command" fd offset size;
			lwt () = resp_wrap c (c_write fd offset size) in
			processor c
		| Cmd.Unlink path ->
			debug "Got Unlink (%s) command" path;
			lwt () = resp_wrap c (c_unlink path) in
			processor c
		| Cmd.Truncate (fd, len) ->
			debug "Got Truncate (%Li) command" len;
			lwt () = resp_wrap c (c_truncate fd len) in
			processor c
		| Cmd.MkDir path ->
			debug "Got MkDir (%s) command" path;
			lwt () = resp_wrap c (c_mkdir path) in
			processor c
		| Cmd.RmDir path ->
			debug "Got RmDir (%s) command" path;
			lwt () = resp_wrap c (c_rmdir path) in
			processor c
		| Cmd.FClose fd ->
			debug "Got FClose (%Li) command" fd;
			lwt () = resp_wrap c (c_fclose fd) in
			processor c
		| Cmd.Rename (oname, nname) ->
			debug "Got Rename ('%s', '%s') command" oname nname;
			lwt () = resp_wrap c (c_rename oname nname) in
			processor c

let acceptor backend connection =
	lwt () = Connection.send_bin_int connection Buildcounter.v in
	debug "Sent bin version";
	let c = {
		connection;
		backend;
		fds = Hashtbl.create 103;
	} in
	try_lwt
		processor c
	with
		| e ->
			debug "Error while processing connection: %s" (Printexc.to_string e);
			drop_connection c


(**********************************************************************************************)


let db_wrap name pool f =
	try
		Db.rw_wrap pool name f
	with
		| Unix.Unix_error (msg, f, _) ->
			fatal (Printf.sprintf "Can't exec query: %s\n" (Unix.error_message msg))


let periodic_statfs pool backend =
	let rec loop () =
		let st = Ostatfs.statfs backend.T_server.storage in
		db_wrap "statfs" pool (fun dbd ->
			Db.RW.exec dbd ("update backend set free_blocks=" ^ (Mysql.ml642int st.Ostatfs.bfree) ^ ", free_files=" ^ (Mysql.ml642int st.Ostatfs.ffree)
				^ " where id=" ^ (Mysql.ml642int backend.T_server.id));
			Lwt.return (Db.Data ())
		);
		Unix.sleep 10;
		loop ()
	in
	loop ()

let daemon pool backend =
	let (_ : Thread.t) = Thread.create (periodic_statfs pool) backend in
	Lwt_main.run (Connection.listen backend.T_server.addr (acceptor backend))

(**********************************************************************************************)
let get_config pool name =
	db_wrap "get_config" pool (fun dbd ->
		try
			let row = Db.RW.select_one dbd ("select id, name, address, port, sum(prio_read), sum(prio_write), storage_dir, free_blocks, free_files, blocks_soft_limit, blocks_hard_limit, files_soft_limit, files_hard_limit from backend where name=" ^ (Mysql.ml2str name)) in
			Lwt.return (Db.Data (Backend.of_db row))
		with
			| Not_found ->
				fatal (Printf.sprintf "Node %s not found in DB\n" name)
	)

let show_nodes () =
	let pool = Db_pool.create !Common.mysql_config in
	db_wrap "show_nodes" pool (fun dbd ->
		let rows = Db.RW.select_all dbd "select id, name, address, port, prio_read, prio_write, storage_dir, free_blocks, free_files, blocks_soft_limit, blocks_hard_limit, files_soft_limit, files_hard_limit  from backend" in
		let rows = List.map Backend.of_db rows in
		let open T_server in
		List.iter (fun b -> Printf.printf "name=%s, storage=%s, addr=%s, port=%i\n" b.name b.storage (Unix.string_of_inet_addr b.addr.inet_addr) b.addr.port) rows;
		exit 0
	)

let args = [
	"--name", Arg.Set_string Config.name, "Backend name (as in 'backend' table)";
	"--show-all", Arg.Unit show_nodes, "Show all nodes";
	"--mysql-host", Arg.String (fun v -> Common.mysql_config := { !Common.mysql_config with Mysql.dbhost = Some v}), "Set MySQL host";
	"--mysql-port", Arg.Int (fun v -> Common.mysql_config := { !Common.mysql_config with Mysql.dbport = Some v}), "Set MySQL port";
	"--mysql-user", Arg.String (fun v -> Common.mysql_config := { !Common.mysql_config with Mysql.dbuser = Some v}), "Set MySQL user name";
	"--mysql-db", Arg.String (fun v -> Common.mysql_config := { !Common.mysql_config with Mysql.dbname = Some v}), "Set MySQL database name";
	"--foreground", Arg.Set Config.foreground, "Run in foreground";
]

let args_usage = "Usage:\n\nbfs_server --name NODE_NAME\n"

let () =
	Arg.parse args (fun s -> raise (Arg.Bad s)) args_usage;
	if !Config.name = "" then (
		Printf.eprintf "%s" (Arg.usage_string args args_usage);
		exit 1
	);
	let pool = Db_pool.create !Common.mysql_config in
	let backend = get_config pool !Config.name in
	debug "Got info from DB";
	begin
		try
			if not (Sys.is_directory backend.T_server.storage) then
				fatal (Printf.sprintf "Storage '%s' is not a directory" backend.T_server.storage)
		with
			| _ ->
				fatal (Printf.sprintf "Storage directory '%s' doesn't exist" backend.T_server.storage)
	end;
	if not !Config.foreground then Lwt_daemon.daemonize ();
	daemon pool backend
