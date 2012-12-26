
module M1 = Le_log

let mysql_config =
	let open Mysql in
	ref {
		defaults with
		dbname = Some "bfs";
		dbuser = Some "bfs";
		dbpwd = Some (Mysql_pwd.get_password ());
	}

let flush_fh_secs = 5.

let backends_update_period = 100

let dead_server_timeout = 5.

let eINVAL = Db.Failure Unix.EINVAL
let eNOSYS = Db.Failure Unix.ENOSYS
let eNOENT = Db.Failure Unix.ENOENT
let eEXIST = Db.Failure Unix.EEXIST
let eAGAIN = Db.Failure Unix.EAGAIN
let eBADF = Db.Failure Unix.EBADF
let eNOTDIR = Db.Failure Unix.ENOTDIR
let eISDIR = Db.Failure Unix.EISDIR
let ePERM = Db.Failure Unix.EPERM

(* --- *)
let eTODO = Db.Failure Unix.ENOSYS

