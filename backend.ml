
let of_db r =
	let open T_server in
	(* id, name, ip, port, prio, dir *)
	let addr = {
		inet_addr = Unix.inet_addr_of_string (Db.to_string r.(2));
		port = Db.to_int r.(3);
		state = Alive;
	} in
	{
		id = Db.to_int64 r.(0);
		name = Db.to_string r.(1);
		addr;
		db_prio_read = Db.to_int r.(4);
		db_prio_write = Db.to_int r.(5);
		local_prio = 0;
		storage = Db.to_string r.(6);
		free_blocks = Db.to_int64 r.(7);
		free_files = Db.to_int64 r.(8);
		free_blocks_soft_limit = Db.to_int64 r.(9);
		free_blocks_hard_limit = Db.to_int64 r.(10);
		free_files_soft_limit = Db.to_int64 r.(11);
		free_files_hard_limit = Db.to_int64 r.(12);
	}

let get_backends dbd id =
	let q = Printf.sprintf "select id, name, address, port, sum(prio_read), sum(prio_write), storage_dir, free_blocks, free_files, blocks_soft_limit, blocks_hard_limit, files_soft_limit, files_hard_limit
		from (
			select b.id, b.name, b.address, b.port, cb.prio_read, cb.prio_write, b.storage_dir, b.free_blocks, b.free_files, b.blocks_soft_limit, b.blocks_hard_limit, b.files_soft_limit, b.files_hard_limit
				from backend b
				left join client_backend cb on b.id=cb.backend
				where cb.client=%Li
			union select id, name, address, port, prio_read, prio_write, storage_dir, free_blocks, free_files, blocks_soft_limit, blocks_hard_limit, files_soft_limit, files_hard_limit
				from backend
			union select b.id, b.name, b.address, b.port, sum(cbg.prio_read), sum(cbg.prio_write), b.storage_dir, b.free_blocks, b.free_files, b.blocks_soft_limit, b.blocks_hard_limit, b.files_soft_limit, b.files_hard_limit
				from backend b
				left join backend_bgroup bbg on bbg.backend=b.id
				left join bgroup bg on bg.id=bbg.bgroup
				left join client_bgroup cbg on cbg.bgroup=bg.id
				where cbg.client=%Li
				group by b.id
			union select b.id, b.name, b.address, b.port, sum(bg.prio_read), sum(bg.prio_write), b.storage_dir, b.free_blocks, b.free_files, b.blocks_soft_limit, b.blocks_hard_limit, b.files_soft_limit, b.files_hard_limit
				from backend b
				left join backend_bgroup bbg on bbg.backend=b.id
				left join bgroup bg on bg.id=bbg.bgroup
				group by b.id
			) as t
		group by id" id id
	in
	let lst = Db.RO.select_all dbd q in
	List.map of_db lst

let backends = ref []

let best_for_create = ref []

let best_for_read = ref []

let sort_read lst =
	let open T_server in
	List.sort (fun a b -> compare (b.db_prio_read + b.local_prio) (a.db_prio_read + b.local_prio)) lst

let sort_create lst =
	let open T_server in
	let q v =
		(if v.free_blocks < v.free_blocks_soft_limit then Common.bfree_soft_limit_score else 0)
		+ (if v.free_blocks < v.free_blocks_hard_limit then Common.bfree_hard_limit_score else 0)
		+ (if v.free_files < v.free_files_soft_limit then Common.ffree_soft_limit_score else 0)
		+ (if v.free_files < v.free_files_hard_limit then Common.ffree_hard_limit_score else 0)
		+ v.db_prio_write + v.local_prio
	in
	List.sort (fun a b -> compare (q b) (q a)) lst

let update_backends dbd id =
	let nlst = get_backends dbd id in
	let open T_server in
	(* унаследовать свойства от уже существующего списка *)
	let nlst = List.map (fun b ->
		try
			let ob = List.find (fun ob -> ob.id = b.id ) !backends in
			{ b with
				local_prio = ob.local_prio;
			} 
		with
			| _ -> b
		
	) nlst in
	backends := nlst;
	best_for_create := sort_create nlst;
	best_for_read := sort_read nlst

let of_backend_id id =
	List.find (fun b -> b.T_server.id = id) !backends

let of_reg_backend row =
	List.find (fun b -> b.T_server.id = row.Dbt.RegBackend_S.backend) !backends

let rec map_reg_backends = function
	| hd :: tl ->
		begin
			try
				of_reg_backend hd :: map_reg_backends tl
			with
			| _ ->
				map_reg_backends tl
		end
	| [] -> []

(* TODO: round-roubin *)
let get_best_for_read rows = sort_read (map_reg_backends rows)

let get_best_for_create cnt =
	let rec loop n = function
		| [] -> []
		| hd :: tl when n > 0 ->
			hd :: loop (n-1) tl
		| _ -> []
	in
	loop cnt !best_for_create
