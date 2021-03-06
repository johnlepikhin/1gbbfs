module LL = Le_log

open T_db

type 'a result =
	| Data of 'a
	| Failure of Unix.error

module RO_RW_Common = struct
	type t = T_db.dbd

	let query dbd q =
		debug "TR ID %i MySQL query: %s" dbd.tr_id q;
		Mysql.exec dbd.dbd q

	let exec ?(lock=true) dbd q =
		if lock then
			Mutex.lock dbd.mutex;
		let unlock () = if lock then Mutex.unlock dbd.mutex else () in
		try
			let _ = query dbd q in
			unlock ();
		with
			| e ->
				unlock ();
				raise e

	let select_one ?(lock=true) dbd q =
		if lock then
			Mutex.lock dbd.mutex;
		let unlock () = if lock then Mutex.unlock dbd.mutex else () in
		try
			let r = query dbd q in
			match Mysql.fetch r with
				| None ->
					unlock ();
					raise Not_found
				| Some row ->
					unlock ();
					row
		with
			| e ->
				unlock ();
				raise e
	

	let select_all ?(lock=true) dbd q =
		if lock then
			Mutex.lock dbd.mutex;
		let unlock () = if lock then Mutex.unlock dbd.mutex else () in
		try
			let r = query dbd q in
			let rec loop lst =
				match Mysql.fetch r with
					| None ->
						lst
					| Some row ->
						loop (row :: lst)
			in
			let lst = loop [] in
			unlock ();
			List.rev lst
		with
			| e ->
				unlock ();
				raise e

	let iter_all ?(lock=true) dbd q f =
		if lock then
			Mutex.lock dbd.mutex;
		let unlock () = if lock then Mutex.unlock dbd.mutex else () in
		try
			let r = query dbd q in
			let rec loop () =
				match Mysql.fetch r with
					| None ->
						Lwt.return ()
					| Some row ->
						lwt () = f row in
						loop ()
			in
			try_lwt
				lwt () = loop () in
				unlock ();
				Lwt.return ()
			with
				| e ->
					unlock ();
					Lwt.fail e
		with
			| e ->
				unlock ();
				raise e

	let get = Db_pool.get
	let release = Db_pool.release

	let ping t =
		Mutex.lock t.mutex;
		try
			Mysql.ping t.dbd;
		with
			| e ->
				Mutex.unlock t.mutex;
				raise e

	let of_dbd t = t

	let to_dbd t = t

	let insert ?(lock=true) t q =
		if lock then
			Mutex.lock t.mutex;
		let unlock () = if lock then Mutex.unlock t.mutex else () in
		try
			let _ = query t q in
			let id = Mysql.insert_id t.dbd in
			unlock ();
			id
		with
			| e ->
				unlock ();
				raise e
end

module RO : sig
		type t

		val get: Db_pool.t -> t
		val release: Db_pool.t -> t -> unit
		val select_one: ?lock : bool -> t -> string -> string option array
		val select_all: ?lock : bool -> t -> string -> string option array list
		val iter_all: ?lock : bool -> t -> string -> (string option array -> unit Lwt.t) -> unit Lwt.t
		val ping: t -> unit
		val to_dbd : t -> T_db.dbd
		val of_dbd : T_db.dbd -> t
	end = struct
		include RO_RW_Common
	end

module RW : sig
		type t

		val get: Db_pool.t -> t
		val release: Db_pool.t -> t -> unit
		val select_one: ?lock : bool -> t -> string -> string option array
		val select_all: ?lock : bool -> t -> string -> string option array list
		val iter_all: ?lock : bool -> t -> string -> (string option array -> unit Lwt.t) -> unit Lwt.t
		val exec: ?lock : bool -> t -> string -> unit
		val insert: ?lock : bool -> t -> string -> int64
		val ping: t -> unit
		val to_dbd : t -> dbd
		val of_dbd : dbd -> t
		val to_ro : t -> RO.t
	end = struct
		include RO_RW_Common

		let to_ro t =
			RO.of_dbd t
	end

let to_string = function
	| Some s -> s
	| None -> raise Not_found

let to_int v = int_of_string (to_string v)
let to_int64 v = Int64.of_string (to_string v)

let to_float v = float_of_string (to_string v)

let to_bool v = match to_string v with | "0" -> false | _ -> true

let like_escape =
	let rex1 = Pcre.regexp "%" in
	let rex2 = Pcre.regexp "_" in
	fun s ->
		let s = Mysql.escape s in
		let s = Pcre.qreplace ~rex:rex1 ~templ:"\\%" s in
		let s = Pcre.qreplace ~rex:rex2 ~templ:"\\_" s in
		s

let _wrap
	~f_get
	~f_ping
	~f_release
	~f_start
	~f_commit
	~f_rollback
	~f_dbd
	~pool ~fname f
=
(*	Gc.compact (); *)
	debug "Call to %s" fname;
	let r =
		try
			let open Mysql in
			let dbd = f_get pool in
			try
(*				f_ping dbd; *)
				f_start dbd;
				let r =
					let rec loop () =
						try
							Lwt_main.run (f dbd)
						with
							| Not_found ->
								debug "TR ID %i Caught general Not_found exception" (Obj.magic dbd).T_db.tr_id;
								Failure Unix.ENOENT
							| e ->
								debug "TR ID %i Caught exception: %s" (Obj.magic dbd).T_db.tr_id (Printexc.to_string e);
								Failure Unix.EAGAIN
					in
					loop ()
				in
				begin
					match r with
						| Data v ->
							f_commit dbd
						| Failure e ->
							f_rollback dbd
				end;
				f_release pool dbd;
				r
	
			with
				| e ->
					f_release pool dbd;
					raise e
		with
			| Db_pool.ConnectError s ->
				Printf.eprintf "%s\n" s;
				exit 1
			| e ->
				debug "Caught fatal wrapper exception: %s" (Printexc.to_string e);
				exit 1
	in
	match r with
		| Data v ->
			debug "Data OK";
			v
		| Failure e ->
			let err = Unix.Unix_error (e, "", "") in
			debug "Failure: %s" (Unix.error_message e);
			raise err

(*
let null_wrap pool fname f = _wrap
	~f_get:(fun _ -> ())
	~f_release:(fun _ _ -> ())
	~f_ping:(fun _ -> ())
	~f_start:(fun () -> ())
	~f_commit:(fun dbd -> ())
	~f_rollback:(fun dbd -> ())
	~pool ~fname f
*)

let ro_wrap (pool : Db_pool.t) fname (f : RO.t -> 'a result Lwt.t) = _wrap
	~f_get:RO.get
	~f_release:RO.release
	~f_ping:RO.ping
	~f_start:(fun _ -> ())
	~f_commit:(fun _ -> ())
	~f_rollback:(fun _ -> ())
	~f_dbd:RO.to_dbd
	~pool ~fname f

let rw_wrap pool fname f = _wrap
	~f_get:RW.get
	~f_release:RW.release
	~f_ping:RW.ping
	~f_start:(fun dbd -> RW.exec dbd "start transaction")
	~f_commit:(fun dbd -> RW.exec dbd "commit")
	~f_rollback:(fun dbd -> RW.exec dbd "rollback")
	~f_dbd:RW.to_dbd
	~pool ~fname f

