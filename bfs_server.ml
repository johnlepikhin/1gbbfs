
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

	let attach t =
		Mutex.lock mutex;
		t.refs <- t.refs + 1;
		Mutex.unlock mutex

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
}

module Config = struct
	let name = ref ""
end

let fatal s =
	Printf.eprintf "Fatal: %s\n" s;
	exit 1

let c_fopen path flags c =
	try
		let id = FD.id_of_name path in
		Connection.send_value c.connection (Response.Data id)
	with
		| Not_found ->
			let fullpath = c.backend.T_server.storage ^ path in
			let fd = Unix.openfile fullpath flags 0o644 in
			let id = FD.add path fd in
			debug "send fd ID=%Li" id;
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
			Connection.send_value c.connection (Response.Data rb);
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
		let buf = Connection.read_string c.connection size in
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

let c_fclose fd c =
	let fd = FD.remove fd in
	(match fd with
		| None -> ()
		| Some fd ->
			Unix.close fd
	);
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
	Connection.disconnect c.connection

let rec processor c =
	try
		Gc.full_major ();
		debug "Waiting for a command";
		let cmd = Connection.read_value ~timeout:864000. c.connection in
		let continue = match cmd with
			| Cmd.Bye ->
				debug "Got Bye command";
				false
			| Cmd.FOpen (path, flags) ->
				debug "Got FOpen '%s' command" path;
				resp_wrap c (c_fopen path flags);
				true
			| Cmd.Read (fd, offset, size) ->
				debug "Got Read (%Li, %Li, %i) command" fd offset size;
				resp_wrap c (c_read fd offset size);
				true
			| Cmd.Write (fd, offset, size) ->
				debug "Got Write (%Li, %Li, %i) command" fd offset size;
				resp_wrap c (c_write fd offset size);
				true
			| Cmd.Unlink path ->
				debug "Got Unlink (%s) command" path;
				resp_wrap c (c_unlink path);
				true
			| Cmd.Truncate (fd, len) ->
				debug "Got Truncate (%Li) command" len;
				resp_wrap c (c_truncate fd len);
				true
			| Cmd.MkDir path ->
				debug "Got MkDir (%s) command" path;
				resp_wrap c (c_mkdir path);
				true
			| Cmd.RmDir path ->
				debug "Got RmDir (%s) command" path;
				resp_wrap c (c_rmdir path);
				true
			| Cmd.FClose fd ->
				debug "Got FClose (%Li) command" fd;
				resp_wrap c (c_fclose fd);
				true
			| Cmd.Rename (oname, nname) ->
				debug "Got Rename ('%s', '%s') command" oname nname;
				resp_wrap c (c_rename oname nname);
				true
		in
		if continue then
			processor c
		else
			drop_connection c
	with
		| e ->
			debug "Error while processing connection: %s" (Printexc.to_string e);
			drop_connection c

let acceptor backend connection =
	Connection.send_bin_int connection Buildcounter.v;
	let c = {
		connection;
		backend;
	} in
	processor c

let db_wrap name pool f =
	try
		Db.ro_wrap pool name f
	with
		| Unix.Unix_error (msg, f, _) ->
			fatal (Printf.sprintf "Can't get info from DB: %s\n" (Unix.error_message msg))


let get_config pool name =
	db_wrap "get_config" pool (fun dbd ->
		try
			let row = Db.RO.select_one dbd ("select id, name, address, port, sum(prio_read), sum(prio_write), storage_dir from backend where name=" ^ (Mysql.ml2str name)) in
			Db.Data (Backend.of_db row)
		with
			| Not_found ->
				fatal (Printf.sprintf "Node %s not found in DB\n" name)
	)

let show_nodes () =
	let pool = Db_pool.create Common.config in
	db_wrap "show_nodes" pool (fun dbd ->
		let rows = Db.RO.select_all dbd "select id, name, address, port, prio_read, prio_write, storage_dir from backend" in
		let rows = List.map Backend.of_db rows in
		let open T_server in
		List.iter (fun b -> Printf.printf "name=%s, storage=%s, addr=%s, port=%i\n" b.name b.storage (Unix.string_of_inet_addr b.addr.inet_addr) b.addr.port) rows;
		exit 0
	)

let args = [
	"--name", Arg.Set_string Config.name, "Backend name (as in 'backend' table)";
	"--show-all", Arg.Unit show_nodes, "Show all nodes"
]

let args_usage = "Usage:\n\nbfs_server --name NODE_NAME\n"

let () =
	Arg.parse args (fun s -> raise (Arg.Bad s)) args_usage;
	if !Config.name = "" then (
		Printf.eprintf "%s" (Arg.usage_string args args_usage);
		exit 1
	);
	let pool = Db_pool.create Common.config in
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
	Connection.listen backend.T_server.addr (acceptor backend)
