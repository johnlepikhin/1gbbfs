exception NotAvailable

module ConnSet = Set.Make(struct type t = T_connection.t let compare = compare end)

type t = {
	server_addr : T_server.addr;
	mutable free : T_connection.t Stack.t;
	mutable in_use : ConnSet.t;
	mutex : Mutex.t;
}

let create server_addr =
	{
		server_addr;
		free = Stack.create ();
		in_use = ConnSet.empty;
		mutex = Mutex.create ();
	}

let get pool =
	Mutex.lock pool.mutex;
	try
		let conn = Stack.pop pool.free in
		pool.in_use <- ConnSet.add conn pool.in_use;
		Mutex.unlock pool.mutex;
		Lwt.return conn
	with
		| _ ->
			let try_connect () =
				try_lwt
					lwt conn = Connection.connect pool.server_addr in
					pool.in_use <- ConnSet.add conn pool.in_use;
					Mutex.unlock pool.mutex;
					Lwt.return conn
				with
					| e ->
						Mutex.unlock pool.mutex;
						pool.server_addr.T_server.state <- T_server.DeadFrom (Unix.gettimeofday ());
						Lwt.fail e
			in
			let open T_server in
			match pool.server_addr.state with
				| Alive -> try_connect ()
				| DeadFrom tm ->
					let now = Unix.gettimeofday () in
					if tm +. Common.dead_server_timeout < now then
						try_connect ()
					else
					begin
						Mutex.unlock pool.mutex;
						Lwt.fail NotAvailable
					end

let release pool conn =
	Mutex.lock pool.mutex;
	try
		pool.in_use <- ConnSet.remove conn pool.in_use;
		Stack.push conn pool.free;
		Mutex.unlock pool.mutex
	with
		| _ ->
			Mutex.unlock pool.mutex

let remove pool conn =
	Mutex.lock pool.mutex;
	try
		pool.in_use <- ConnSet.remove conn pool.in_use;
		lwt () = Lwt_unix.close conn.T_connection.socket in
		Mutex.unlock pool.mutex;
		Lwt.return ()
	with
		| _ ->
			Mutex.unlock pool.mutex;
			Lwt.return ()

let conn_cache = Hashtbl.create 1003

let conn_cache_mutex = Mutex.create ()

let wrap ~f addr =
	let try_pool pool =
		let addr_string = Printf.sprintf "%s:%i" (Unix.string_of_inet_addr addr.T_server.inet_addr) addr.T_server.port in
		let error s =
			debug "Can't connect to server %s : %s" addr_string s;
			Lwt.fail NotAvailable
		in
		try_lwt
			lwt conn = get pool in
			try_lwt
				debug "Call to addr %s" addr_string;
				lwt r = f conn in
				release pool conn;
				Lwt.return r
			with
				| Connection.Timeout
				| End_of_file ->
					debug "Connection to server dead";
					lwt () = remove pool conn in
					Lwt.fail Connection.Timeout
				| e ->
					debug "Can't connect to server: %s" (Printexc.to_string e);
					release pool conn;
					Lwt.fail e
		with
			| Unix.Unix_error (ue, _, _) ->
				error (Unix.error_message ue)
			| e ->
				error (Printexc.to_string e)
	in
	try
		Mutex.lock conn_cache_mutex;
		let pool = Hashtbl.find conn_cache addr in
		Mutex.unlock conn_cache_mutex;
		try_pool pool
	with
		| NotAvailable ->
			raise NotAvailable
		(* pool doesn't exist in hash table *)
		| _ ->
			let pool = create addr in
			Hashtbl.add conn_cache addr pool;
			Mutex.unlock conn_cache_mutex;
			try_pool pool

type 'a callback = backend : T_server.server -> reg_backend : Dbt.RegBackend.row -> row : 'a -> T_connection.t -> unit Lwt.t

let do_for_all_p
	?(on_error=(fun ~reg_backend ~row _ -> Lwt.return ()))
	~(getter : 'a -> Dbt.RegBackend.row)
	~(f : 'a callback)
	(rows : 'a list)
=
	let call row =
		let reg_backend = getter row in
		try_lwt
			let backend = Backend.of_reg_backend reg_backend in
			wrap ~f:(f ~backend ~reg_backend ~row) backend.T_server.addr
(*
			let backend = try Some (Backend.of_reg_backend reg_backend) with | _ -> None in
			match backend with
				| Some backend ->
					wrap ~f:(f ~backend ~reg_backend ~row) backend.T_server.addr;
				| None ->
					()
*)
		with
			| e ->
				debug "Error for reg_backend row id=%Li: %s" reg_backend.Dbt.RegBackend_S.id (Printexc.to_string e);
				try_lwt
					on_error ~reg_backend ~row e
				with
					| _ -> Lwt.return ()
	in
	let tids = List.map call rows in
	Lwt.join tids

let do_for_all_s
	?(on_error=(fun ~reg_backend ~row _ -> Lwt.return true))
	~(getter : 'a -> Dbt.RegBackend.row)
	~f
	(rows : 'a list)
=
	let call row =
		let reg_backend = getter row in
		try_lwt
			let backend = Backend.of_reg_backend reg_backend in
			wrap ~f:(f ~backend ~reg_backend ~row) backend.T_server.addr
		with
			| e ->
				try_lwt
					on_error ~reg_backend ~row e
				with
					| _ -> Lwt.return true
	in
	let rec loop = function
		| hd :: tl ->
			lwt r = call hd in
			if r then
				loop tl
			else
				Lwt.return ()
		| [] -> Lwt.return ()
	in
	loop rows

let do_on_first ~f ids =
	()

(*
let cleanup pool limit =
	Mutex.lock pool.mutex;
	try
		let len = Stack.length pool.free in
		for i=1 to len-limit do
			try
				let dbd = Stack.pop pool.free in
				Mysql.disconnect dbd
			with
				| _ -> ()
		done;
		Mutex.unlock pool.mutex
	with
		| _ ->
			Mutex.unlock pool.mutex

let cleanup_periodically pool limit =
	let rec loop () =
		Unix.sleep 30;
		cleanup pool limit;
		loop ()
	in
	loop ()
*)
