
open T_server

module Config = struct
	let name = ref ""
end

let add_slash p =
	if p.[(String.length p) - 1] = '/' then p else p ^ "/"


let fatal s =
	Printf.eprintf "Fatal: %s\n" s;
	exit 1

let insert is_dir dbd backend path plen stat =
	let fullname = String.sub path plen ((String.length path) - plen) in
	if fullname <> "/" then
		let metadata = Dbt.Metadata.of_stat stat fullname in
		let metadata = Dbt.Metadata.insert dbd metadata in
		if is_dir then
			()
		else
			let reg_backend = Dbt.RegBackend.create ~metadata:(Some metadata) ~backend fullname in
			let (_ : int64) = Dbt.RegBackend.insert dbd reg_backend in
			()

let rec loop dbd backend path plen =
	debug :1 "loop %s" path;
	let open Unix.LargeFile in
	let s = stat path in
	match s.st_kind with
		| Unix.S_REG ->
			debug :1 "S_REG %s" path;
			insert false dbd backend path plen s
		| Unix.S_DIR ->
			debug :1 "S_DIR %s" path;
			insert true dbd backend path plen s;
			let dh = Unix.opendir path in
			(try
				while true do
					let entry = Unix.readdir dh in
					debug :1 "entry %s" entry;
					if entry <> "." && entry <> ".." then
						loop dbd backend ((add_slash path) ^ entry) plen
				done
			with
				| _ -> ()
			);
			Unix.closedir dh
		| _ ->
			(* not implemented *)
			()

let import pool b =
	Db.rw_wrap pool "import" (fun dbd ->
		let root = add_slash b.T_server.storage in
		let plen = (String.length root) - 1 in
		loop dbd b.T_server.id root plen;
		Lwt.return (Db.Data ())
	)

let db_wrap name pool f =
	try
		Db.rw_wrap pool name f
	with
		| Unix.Unix_error (msg, f, _) ->
			fatal (Printf.sprintf "Can't get info from DB: %s\n" (Unix.error_message msg))


let get_config pool name =
	db_wrap "get_config" pool (fun dbd ->
		try
			let row = Db.RW.select_one dbd ("select id, name, address, port, sum(prio_read), sum(prio_write), storage_dir from backend where name=" ^ (Mysql.ml2str name)) in
			Lwt.return (Db.Data (Backend.of_db row))
		with
			| Not_found ->
				fatal (Printf.sprintf "Node %s not found in DB\n" name)
	)

let show_nodes () =
	let pool = Db_pool.create !Common.mysql_config in
	db_wrap "show_nodes" pool (fun dbd ->
		let rows = Db.RW.select_all dbd "select id, name, address, port, prio_read, prio_write, storage_dir from backend" in
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
]

let args_usage = "Usage:\n\nbfs_import --name NODE_NAME\n"

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
	import pool backend
