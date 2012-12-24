
module M1 = Le_log

exception TryAgain

let config =
	let open Mysql in
	{
		defaults with
		dbname = Some "dbfs";
		dbuser = Some "dbfs";
		dbpwd = Some "q1w2e3";
	}

let flush_fh_secs = 5.

let backends_update_period = 100

let dead_server_timeout = 5.

(*
let server1 =
	let open T_server in
	{
		addr = {
			inet_addr = Unix.inet_addr_loopback;
			port = 54321;
		};
		storage = "/tmp/storage/";
	}
*)

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

