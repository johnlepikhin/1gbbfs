
open Dbt

let q_get_invalid_files backend = "select m.id, m.required_distribution as required, count(r1.id), min(r3.max_valid_pos)
)
	from metadata as m
	left join reg_backend as r1 on r1.metadata = m.id
	left join reg_backend as r2 on r2.metadata = m.id
	left join reg_backend as r3 on r3.metadata = m.id
	where m.type = 0
		and r1.state = 0
		and r2.state = 0 and r2.backend = " ^ (Mysql.ml642int backend) ^ "
	group by m.id
	having count(r1.id) > 0 and count(r1.id) < required
	order by required-count(r1.id) desc
	limit 1000
	for update"

let block_size = 100000L

let redistribute_file can_continue dbd path spos length =
	lwt r = Op_open.c_fopen path [] dbd in
	let buffer = Bigarray.Array1.create Bigarray.char Bigarray.c_layout (Int64.to_int block_size) in
	match r with
		| Db.Data _ ->
			let rec copy spos = function
				| 0L ->
					Lwt.return ()
				| tl ->
					if can_continue () then
					begin
						let bsize = if tl > block_size then block_size else tl in
						lwt r = Op_read.c_read path buffer spos bsize dbd in
						match r with
							| Db.Data rd ->
								lwt r = Op_write.c_write ~do_on:Op_write.Invalid path buffer spos rd dbd in
								begin
									match r with
										| Db.Data wr ->
											let wr = Int64.of_int wr in
											copy (Int64.add spos wr) (Int64.sub tl wr)
										| Db.Failure _ ->
											Lwt.return ()
								end
							| Db.Failure _ ->
								Lwt.return ()
					end
					else
						Lwt.return ()
			in
			let finalize () =
				lwt _ = Op_release.c_release path [] None dbd in
				Lwt.return ()
			in
			begin
				try_lwt
					lwt () = copy spos length in
					finalize ()
				with
					| _ ->
						finalize ()
			end
		| Db.Failure _ ->
			Lwt.return ()

let fix_invalid_files dbd backend =
	let t_start = Unix.gettimeofday () in
	let can_continue () =
		if t_start +. Common.fixer_iteration_timeout < Unix.gettimeofday () then
			false
		else
			true
	in
	Db.RW.iter_all dbd (q_get_invalid_files backend) (fun row ->
		try
			let id = Db.to_int64 row.(0) in
			let required_distribution = Db.to_int row.(1) in
			let start_pos = Db.to_int64 row.(3) in
			let meta = Metadata.rw_select_one dbd ("id=" ^ (Mysql.ml642int id)) in
			let regs = RegBackend.rw_all_of_metadata dbd meta in
			let existing_backends_ids = List.map (fun r -> r.RegBackend_S.id) regs in
			(* TODO надо иначе передавать required_distribution. И вообще иначе назвать параметр *)
			lwt () = Op_create.create_missing_backends ~required_distribution ~meta ~meta_id:meta.Metadata_S.id ~existing_backends_ids dbd in
			redistribute_file can_continue dbd meta.Metadata_S.fullname start_pos (Int64.sub meta.Metadata_S.stat.Unix.LargeFile.st_size start_pos)
		with
			| _ -> Lwt.return ()
	)

let fix_all_backends pool =
	Db.rw_wrap pool "fixer" (fun dbd ->
		lwt () = Lwt_list.iter_s (fun b ->
			lwt () = Lwt_unix.sleep 10. in
			lwt _ = fix_invalid_files dbd b.T_server.id in
			Lwt.return ()
		) !Backend.best_for_read in
		Lwt.return (Db.Data ())
	)
