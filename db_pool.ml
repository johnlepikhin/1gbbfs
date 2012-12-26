type t = {
	auth_info : Mysql.db;
	mutable free : T_db.dbd Stack.t;
	mutable in_use : T_db.dbd list;
	mutex : Mutex.t;
}

exception ConnectError of string

let create auth_info =
	{
		auth_info;
		free = Stack.create ();
		in_use = [];
		mutex = Mutex.create ();
	}

let tr_id_gen =
	let i = ref 0 in
	fun () ->
		incr i;
		!i

let create_and_return pool =
	Mutex.lock pool.mutex;
	try
		let dbd = Mysql.connect pool.auth_info in
		let (_ : Mysql.result) = Mysql.exec dbd "set session transaction isolation level serializable" in
		let dbd = {
			T_db.dbd = dbd;
			T_db.mutex = Mutex.create ();
			T_db.tr_id = 0;
		} in
		pool.in_use <- dbd :: pool.in_use;
		Mutex.unlock pool.mutex;
		dbd
	with
		| Mysql.Error s ->
			Mutex.unlock pool.mutex;
			raise (ConnectError s)
		| e ->
			Mutex.unlock pool.mutex;
			raise e

let get pool =
	Mutex.lock pool.mutex;
	try
		let dbd = Stack.pop pool.free in
		pool.in_use <- dbd :: pool.in_use;
		dbd.T_db.tr_id <- tr_id_gen ();
		Mutex.unlock pool.mutex;
		dbd
	with
		| _ ->
			Mutex.unlock pool.mutex;
			create_and_return pool

let release pool dbd =
	Mutex.lock pool.mutex;
	try
		pool.in_use <- List.filter (fun e -> e != dbd) pool.in_use;
		Stack.push dbd pool.free;
		Mutex.unlock pool.mutex
	with
		| _ ->
			Mutex.unlock pool.mutex

let cleanup pool limit =
	Mutex.lock pool.mutex;
	try
		let len = Stack.length pool.free in
		for i=1 to len-limit do
			try
				let dbd = Stack.pop pool.free in
				Mysql.disconnect dbd.T_db.dbd
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
