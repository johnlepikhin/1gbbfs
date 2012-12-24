
type backend = {
	regbackend : Dbt.RegBackend.row;
	server : T_server.server;
	remote_fd : int64 option;
}

type t = {
	mutable metadata : Dbt.Metadata.row;
	mutable backends : backend list;
	mutable open_counter : int;
	mutex : Mutex.t;
	mutable is_closed : bool;
}

let global_lock = Mutex.create ()

let tbl = Hashtbl.create 1003

let unsafe_remove path =
	Hashtbl.remove tbl path

let remove path =
	try
		let t = Hashtbl.find tbl path in
		Mutex.lock t.mutex;
		if t.open_counter = 0 then
			Hashtbl.remove tbl path;
		Mutex.unlock t.mutex
	with
		| _ -> ()

let get = Hashtbl.find tbl

(*
let mem = Hashtbl.mem tbl

let get_for_reading path =
	let t = Hashtbl.find tbl path in
	let b = Queue.take t.backends in
	Queue.add b t.backends;
	b

let get_for_writing path =
	let t = Hashtbl.find tbl path in
	let lst = ref [] in
	Queue.iter (fun b -> lst := b :: !lst) t.backends;
	!lst
*)

let add_backend fh regbackend server remote_fd =
	let b = {
		regbackend;
		server;
		remote_fd;
	} in
	fh.backends <- b :: fh.backends;;

let soft_open_file path =
	try
		Mutex.lock global_lock;
		let t = get path in
		Mutex.lock t.mutex;
		if t.is_closed then (
			Mutex.unlock t.mutex;
			Mutex.unlock global_lock;
			false
		) else (
			t.open_counter <- t.open_counter + 1;
			Mutex.unlock t.mutex;
			Mutex.unlock global_lock;
			true
		)
	with
		| _ ->
			Mutex.unlock global_lock;
			false

(* need to unlock t.mutex after file is really opened! *)
let open_file metadata =
	debug "open_file 1";
	try
	debug "open_file 2";
		Mutex.lock global_lock;
	debug "open_file 3";
		let t = get metadata.Dbt.Metadata_S.fullname in
	debug "open_file 4";
		Mutex.lock t.mutex;
	debug "open_file 5";
		if t.is_closed then (
	debug "open_file 6";
			Mutex.unlock t.mutex;
	debug "open_file 7";
			raise Not_found
		) else (
	debug "open_file 8";
			t.open_counter <- t.open_counter + 1;
	debug "open_file 9";
			Mutex.unlock global_lock;
	debug "open_file 11";
			t
		)
	with
		| _ ->
	debug "open_file 12";
			let t = {
				metadata;
				backends = [];
				open_counter = 1;
				mutex = Mutex.create ();
				is_closed = false;
			} in
	debug "open_file 13";
			Hashtbl.add tbl metadata.Dbt.Metadata_S.fullname t;
	debug "open_file 14";
			Mutex.lock t.mutex;
	debug "open_file 15";
			Mutex.unlock global_lock;
	debug "open_file 16";
			t

let close_file path =
	try
		Mutex.lock global_lock;
		let t = get path in
		Mutex.lock t.mutex;
		t.open_counter <- t.open_counter - 1;
		if t.open_counter = 0 then
		begin
			t.metadata <- { t.metadata with Dbt.Metadata_S.fh_counter = t.metadata.Dbt.Metadata_S.fh_counter - 1 };
			t.is_closed <- true;
			Mutex.unlock t.mutex;
			Some t
		end
		else
		begin
			Mutex.unlock t.mutex;
			None
		end
	with
		| _ ->
			None

let get_all_backends fd =
	fd.backends

let get_valid_backends fd =
	let open Dbt.RegBackend_S in
	List.filter (fun b -> b.regbackend.state = Valid) fd.backends

let db_sync dbd fd =
	let open Dbt in
	let m = Metadata.rw_of_path dbd fd.metadata.Metadata_S.fullname in
	let size = fd.metadata.Metadata_S.stat.Unix.LargeFile.st_size in
	List.iter (fun b -> RegBackend.update_by_key dbd b.regbackend) fd.backends;
	let open Dbt.Metadata_S in
	Dbt.Metadata.update_by_key dbd { m with stat = { m.stat with Unix.LargeFile.st_size = size }; fh_counter = fd.metadata.fh_counter }
