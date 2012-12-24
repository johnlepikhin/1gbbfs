

module type TABLE = sig
	type row

	type key

	val cols: string list

	val name: string

	val key_name: string

	val cache_timeout: float

	val of_db: string option array -> row

	val to_db: row -> (string * string) list

	val key_of_row: row -> key

	val escaped_key_of_row: row -> string
end

let periodic_f = ref []

let rec periodic_processor () =
	Unix.sleep 5;
	debug "Cleanup DBT caches";
	List.iter (fun f -> f ()) !periodic_f;
	periodic_processor ()

module Cache = struct
	type ('key, 'row) t = {
		cache : ('key, ('row * float)) Hashtbl.t;
		mutex : Mutex.t;
	}

	let create () = {
		cache = Hashtbl.create 103;
		mutex = Mutex.create ();
	}

	let find t key =
		Mutex.lock t.mutex;
		try
			let r = Hashtbl.find t.cache key in
			Mutex.unlock t.mutex;
			r
		with
			| e ->
				Mutex.unlock t.mutex;
				raise e

	let add t key v =
		Mutex.lock t.mutex;
		Hashtbl.replace t.cache key v;
		Mutex.unlock t.mutex

	let remove t key =
		Mutex.lock t.mutex;
		Hashtbl.remove t.cache key;
		Mutex.unlock t.mutex

end

module F = functor (T : TABLE) ->
	struct
		type row = T.row

		type key = T.key

		let name = T.name

		let cols_string = String.concat ", " T.cols

		let cache = Cache.create ()

		let lookup_cache key =
			try
				let (row, tm) = Cache.find cache key in
				let now = Unix.gettimeofday () in
				if tm +. T.cache_timeout > now then
					Some row
				else
					None
			with
				| _ -> None

		let add_cache row =
			let now = Unix.gettimeofday () in
			Cache.add cache (T.key_of_row row) (row, now)

		let remove_cache row =
			Cache.remove cache (T.key_of_row row)

		let ro_select_one ?(for_update=true) ?(key : key option) dbd where =
			let get () =
				let for_update = if for_update then " for update" else "" in
				let r = Db.RO.select_one dbd ("select " ^ cols_string ^ " from " ^ T.name ^ " where " ^ where ^ for_update) in
				T.of_db r
			in
			let get_and_cache () =
				let row = get () in
				add_cache row;
				row
			in
			match key with
				| Some key -> (
					match lookup_cache key with
						| Some row -> row
						| None -> get_and_cache ()
				)
				| None -> get ()

		let rw_select_one ?for_update ?key dbd where =
			let dbd = Db.RW.to_ro dbd in
			ro_select_one ?for_update ?key dbd where

		let ro_select_all ?(for_update=true) dbd where =
			let for_update = if for_update then " for update" else "" in
			let r = Db.RO.select_all dbd ("select " ^ cols_string ^ " from " ^ T.name ^ " where " ^ where ^ for_update) in
			let rows = List.map T.of_db r in
			let now = Unix.gettimeofday () in
			List.iter (fun row -> Cache.add cache (T.key_of_row row) (row, now)) rows;
			(rows : row list)

		let rw_select_all ?for_update dbd where =
			let dbd = Db.RW.to_ro dbd in
			ro_select_all ?for_update dbd where

		let string_of_row row =
			let vals = T.to_db row in
			String.concat ", " (List.map (fun (k,v) -> k ^ "=" ^ v) vals)

		let insert dbd row =
			Db.RW.insert dbd ("insert into " ^ T.name ^ " set " ^ (string_of_row row))

		let update_where dbd row where =
			Db.RW.exec dbd ("update " ^ T.name ^ " set " ^ (string_of_row row) ^ " where " ^ where);
			add_cache row

		let update_by_key dbd row =
			Db.RW.exec dbd ("update " ^ T.name ^ " set " ^ (string_of_row row) ^ " where " ^ T.key_name ^ "=" ^ (T.escaped_key_of_row row));
			add_cache row

		let delete dbd row =
			Db.RW.exec dbd ("delete from " ^ T.name ^ " where " ^ T.key_name ^ "=" ^ (T.escaped_key_of_row row));
			remove_cache row

		let delete_where dbd where =
			Db.RW.exec dbd ("delete from " ^ T.name ^ " where " ^ where)
			(* TODO? remove_cache row *)

		let clean_cache () =
			let now = Unix.gettimeofday () in
			let f k (_, tm) =
				if tm +. T.cache_timeout <= now then
					Hashtbl.remove cache.Cache.cache k
			in
			Mutex.lock cache.Cache.mutex;
			Hashtbl.iter f cache.Cache.cache;
			Mutex.unlock cache.Cache.mutex

		let () =
			periodic_f := clean_cache :: !periodic_f
	end

module Metadata_S =
	struct
		type row = {
			id : int64;
			mutable stat : Unix.LargeFile.stats;
			fullname : string;
			deepness : int;
			fh_counter : int;
			required_distribution : int;
		}

		type key = string (* fullname *)

		let cols = [ "id"; "fullname"; "mode"; "uid"; "gid"; "size"; "mtime"; "type"; "deepness"; "fh_counter"; "required_distribution" ]

		let name = "metadata"

		let key_name = "fullname"

		let cache_timeout = 1.

		let of_db row =
			let open Unix.LargeFile in
			let stat = {
				st_dev = 1;
				st_ino = 1;
				st_kind = Kind.of_int (Db.to_int row.(7));
				st_perm = Db.to_int row.(2);
				st_nlink = 1;
				st_uid = Db.to_int row.(3);
				st_gid = Db.to_int row.(4);
				st_rdev = 1;
				st_size = Db.to_int64 row.(5);
				st_atime = 0.;
				st_mtime = Db.to_float row.(6);
				st_ctime = 0.;
			} in
			let r = {
				stat;
				id = Db.to_int64 row.(0);
				fullname = Db.to_string row.(1);
				deepness = Db.to_int row.(8);
				fh_counter = Db.to_int row.(9);
				required_distribution = Db.to_int row.(10);
			} in
			r

		let to_db row =
			let open Unix.LargeFile in
			[
				"fullname", Mysql.ml2str row.fullname;
				"mode", Mysql.ml2int row.stat.st_perm;
				"uid", Mysql.ml2int row.stat.st_uid;
				"gid", Mysql.ml2int row.stat.st_gid;
				"size", Mysql.ml642int row.stat.st_size;
				"mtime", Mysql.ml2float row.stat.st_mtime;
				"type", Mysql.ml2int (Kind.to_int row.stat.st_kind);
				"deepness", Mysql.ml2int row.deepness;
				"fh_counter", Mysql.ml2int row.fh_counter;
				"required_distribution", Mysql.ml2int row.required_distribution;
			]

		let key_of_row row = row.fullname

		let escaped_key_of_row row = Mysql.ml2str row.fullname
	end

module Metadata =
	struct
		include F(Metadata_S)

		let ro_of_path ?for_update ?(use_cache=false) dbd path =
			if use_cache then
				ro_select_one ?for_update ~key:path dbd ("fullname=" ^ (Mysql.ml2str path))
			else
				ro_select_one ?for_update dbd ("fullname=" ^ (Mysql.ml2str path))

		let rw_of_path ?for_update ?use_cache dbd path =
			let dbd = Db.RW.to_ro dbd in
			ro_of_path ?for_update ?use_cache dbd path

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
			let open Metadata_S in
			{
				id = 0L;
				stat = default_stat;
				fullname = "";
				deepness = 0;
				fh_counter = 0;
				required_distribution = 1;
			}

		let create st_kind st_perm path =
			let open Unix.LargeFile in
			let open Metadata_S in
			{ default with
				stat = { default_stat with st_mtime = Unix.time (); st_kind; st_perm };
				fullname = path;
			}

		let directory = create Unix.S_DIR

		let regular = create Unix.S_REG

		let set_size ~size row =
			let open Metadata_S in
			let open Unix.LargeFile in
			let stat = { row.stat with st_size = size } in
			row.stat <- stat

		let grow ~size row =
			let open Metadata_S in
			let open Unix.LargeFile in
			if row.stat.st_size < size then
				set_size ~size row

		let shrink ~size row =
			let open Metadata_S in
			let open Unix.LargeFile in
			if row.stat.st_size > size then
				set_size ~size row
	end

module RegBackend_S =
	struct
		type state =
			| Valid
			| NeedSync
			| Deleted
			| Immutable

		type row = {
			id : int64;
			metadata : int64 option;
			backend : int64;
			path : string;
			mutable max_valid_pos : int64;
			mutable state : state;
			mutable metadata_update_tries : int;
		}

		let state_of_db v =
			match Db.to_int v with
				| 0 -> Valid
				| 1 -> NeedSync
				| 2 -> Deleted
				| 3 -> Immutable
				| _ ->
					debug "Invalid reg_backend state";
					raise Not_found

		let db_of_state = function
			| Valid -> 0
			| NeedSync -> 1
			| Deleted -> 2
			| Immutable -> 3

		type key = int64 (* id *)

		let cols = [ "id"; "metadata"; "backend"; "path"; "max_valid_pos"; "state"; "metadata_update_tries" ]

		let name = "reg_backend"

		let key_name = "id"

		let cache_timeout = 1.

		let of_db row =
			let r = {
				id = Db.to_int64 row.(0);
				metadata = Mysql.opt Mysql.int642ml row.(1);
				backend = Db.to_int64 row.(2);
				path = Db.to_string row.(3);
				max_valid_pos = Db.to_int64 row.(4);
				state = state_of_db row.(5);
				metadata_update_tries = Db.to_int row.(6);
			} in
			r

		let to_db row =
			let open Unix.LargeFile in
			[
				"id", Mysql.ml642int row.id;
				"metadata", (match row.metadata with | Some i -> Mysql.ml642int i | None -> "NULL");
				"backend", Mysql.ml642int row.backend;
				"path", Mysql.ml2str row.path;
				"max_valid_pos", Mysql.ml642int row.max_valid_pos;
				"state", Mysql.ml2int (db_of_state row.state);
				"metadata_update_tries", Mysql.ml2int row.metadata_update_tries;
			]

		let key_of_row row = row.id

		let escaped_key_of_row row = Mysql.ml642int row.id
	end

module RegBackend =
	struct
		include F(RegBackend_S)

		let ro_all_of_metadata dbd metadata =
			ro_select_all dbd ("metadata=" ^ (Mysql.ml642int metadata.Metadata_S.id))

		let rw_all_of_metadata dbd metadata =
			let dbd = Db.RW.to_ro dbd in
			ro_all_of_metadata dbd metadata

		let ro_valid_of_metadata dbd metadata =
			let size = metadata.Metadata_S.stat.Unix.LargeFile.st_size in
			ro_select_all dbd ("metadata=" ^ (Mysql.ml642int metadata.Metadata_S.id) ^ " and max_valid_pos=" ^ (Mysql.ml642int size))

		let rw_valid_of_metadata dbd metadata =
			let dbd = Db.RW.to_ro dbd in
			ro_valid_of_metadata dbd metadata

		let ro_of_metadata_and_backend ~metadata ~backend dbd =
			ro_select_one dbd ("metadata=" ^ (Mysql.ml642int metadata.Metadata_S.id) ^ " and backend=" ^ (Mysql.ml642int backend))

		let rw_of_metadata_and_backend ~metadata ~backend dbd =
			let dbd = Db.RW.to_ro dbd in
			ro_of_metadata_and_backend ~metadata ~backend dbd

		let ro_of_backend_and_path ~backend ~path dbd =
			ro_select_one dbd ("path=" ^ (Mysql.ml2str path) ^ " and backend=" ^ (Mysql.ml642int backend))

		let rw_of_backend_and_path ~backend ~path dbd =
			let dbd = Db.RW.to_ro dbd in
			ro_of_backend_and_path ~backend ~path dbd

		let is_valid metadata row =
			metadata.Metadata_S.stat.Unix.LargeFile.st_size = row.RegBackend_S.max_valid_pos


		let mark_invalid ~from_pos row =
			let open RegBackend_S in
			row.state <- NeedSync;
			if row.max_valid_pos >= from_pos then
				row.max_valid_pos <- Int64.pred from_pos

		let create ~metadata ~backend path =
			let open RegBackend_S in
			{
				id = 0L;
				metadata;
				backend;
				path;
				max_valid_pos = 0L;
				state = Valid;
				metadata_update_tries = 0;
			}
	end

module Client_S =
	struct
		type row = {
			id : int64;
			name : string;
		}

		type key = int64 (* id *)

		let cols = [ "id"; "name" ]

		let name = "client"

		let key_name = "id"

		let cache_timeout = 5.

		let of_db row = {
				id = Db.to_int64 row.(0);
				name = Db.to_string row.(1);
			}

		let to_db row =
			let open Unix.LargeFile in
			[
				"name", Mysql.ml2str row.name;
			]

		let key_of_row row = row.id

		let escaped_key_of_row row = Mysql.ml642int row.id
	end

module Client =
	struct
		include F(Client_S)

		let ro_of_name dbd name =
			ro_select_one dbd ("name=" ^ (Mysql.ml2str name))

		let rw_of_name ?use_cache dbd name =
			let dbd = Db.RW.to_ro dbd in
			ro_of_name dbd name

		let default () = {
			Client_S.id = 0L;
			Client_S.name = "";
		}
	end


