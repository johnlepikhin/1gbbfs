type t = {
	stat : Unix.LargeFile.stats;
	id : int64;
	backend : int64 option;
	fullname : string;
	deepness : int;
	backend_fullname : string option;
	fh_counter : int;
}

let default_stat =
	let open Unix.LargeFile in
	{
		st_dev = 0;
		st_ino = 0;
		st_kind = Unix.S_REG;
		st_perm = 0o644;
		st_nlink = 1;
		st_uid = 0;
		st_gid = 0;
		st_rdev = 0;
		st_size = 0L;
		st_atime = 0.;
		st_mtime = 0.;
		st_ctime = 0.;
	}

let default =
	{
		stat = default_stat;
		id = 0L;
		backend = None;
		fullname = "";
		deepness = 0;
		backend_fullname = None;
		fh_counter = 0;
	}

let split path =
	(Filename.dirname path, Filename.basename path)

let cols = "id, backend, fullname, mode, uid, gid, size, mtime, type, deepness, backend_fullname, fh_counter"

let of_db (row : string option array) =
	let open Unix.LargeFile in
	let st_size = Db.to_int64 row.(6) in
	debug "read size=%Li" st_size;
	let stat = {
		st_dev = 1;
		st_ino = 1;
		st_kind = Kind.of_int (Db.to_int row.(8));
		st_perm = Db.to_int row.(3);
		st_nlink = 1;
		st_uid = Db.to_int row.(4);
		st_gid = Db.to_int row.(5);
		st_rdev = 1;
		st_size;
		st_atime = 0.;
		st_mtime = Db.to_float row.(7);
		st_ctime = 0.;
	} in
	let r = {
		stat;
		id = Db.to_int64 row.(0);
		backend = (match row.(1) with | None -> None | Some s -> Some (Int64.of_string s));
		fullname = Db.to_string row.(2);
		deepness = Db.to_int row.(9);
		backend_fullname = row.(10);
		fh_counter = Db.to_int row.(11);
	} in
	r

let insert_to_db dbd row =
	let open Unix.LargeFile in
	Db.RW.exec dbd ("insert into file set
		backend=" ^ (match row.backend with | None -> "NULL" | Some i -> Mysql.ml642int i) ^ ",
		fullname=" ^ (Mysql.ml2str row.fullname) ^ ", 
		deepness=" ^ (Mysql.ml2int row.deepness) ^ ", 
		backend_fullname=" ^ (match row.backend_fullname with | None -> "NULL" | Some s -> Mysql.ml2str s) ^ ",
		fh_counter=" ^ (Mysql.ml2int row.fh_counter) ^ ", 
		mode=" ^ (Mysql.ml2int row.stat.Unix.LargeFile.st_perm) ^ ", 
		uid=" ^ (Mysql.ml2int row.stat.st_uid) ^ ", 
		gid=" ^ (Mysql.ml2int row.stat.st_gid) ^ ", 
		size=" ^ (Mysql.ml642int row.stat.st_size) ^ ", 
		mtime=" ^ (Mysql.ml2float row.stat.st_mtime) ^ ", 
		type=" ^ (Mysql.ml2int (Kind.to_int row.stat.st_kind)));
	let id = Db.RW.insert_id dbd in
	{ row with id }

let create st_kind st_perm path =
	let open Unix.LargeFile in
	{ default with
		stat = { default_stat with st_mtime = Unix.time (); st_kind; st_perm };
		fullname = path;
	}

let directory = create Unix.S_DIR

(*
let symlink select_one_f mode dest_path link_path =
	let r = create Unix.S_LNK mode select_one_f link_path in
	{ r with sympath = Some dest_path }
*)

let regular = create Unix.S_REG
