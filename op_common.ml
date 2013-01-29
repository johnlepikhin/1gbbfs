open Dbt

let add_slash p =
	if p.[(String.length p) - 1] = '/' then p else p ^ "/"

let get_parent_meta path dbd =
	let parent_path = Filename.dirname path in
	let parent_path = if parent_path = "" then "/" else parent_path in
	Metadata.rw_of_path dbd parent_path

let generate_prefix () =
	Printf.sprintf "__bfs_renamed_%.0f_%i__" (Unix.gettimeofday ()) (Random.int 1000000)

let rec backend_mkdir ?(prefix="") dirname parent_path metadata backend_id dbd =
	let parent_path = add_slash parent_path in
	let try_other_name () =
		let prefix = generate_prefix () in
		backend_mkdir ~prefix dirname parent_path metadata backend_id dbd
	in
	let fullpath = parent_path ^ prefix ^ dirname in
	try_lwt
		(* check if element with such name already exists on backend *)
		let (_ : RegBackend_S.row) = RegBackend.rw_of_backend_and_path ~backend:backend_id ~path:fullpath dbd in
		try_other_name ()
	with
		| Not_found ->
			let r = ref None in
			let open T_communication in
			let f c =
				lwt res = Connection.request c (Cmd.MkDir fullpath) in
				r := res;
				Lwt.return ()
			in
			let backend_srv = Backend.of_backend_id backend_id in
			lwt () = Connection_pool.wrap ~f backend_srv.T_server.addr in
			match !r with
				| Some R_MkDir.OK ->
					let reg = RegBackend.create ~metadata:(Some metadata.Metadata_S.id) ~backend:backend_id fullpath in
					let id = RegBackend.insert dbd reg in
					Lwt.return { reg with RegBackend_S.id = id }
				| Some R_MkDir.AlreadyExists ->
					try_other_name ()
				| None ->
					debug "Call to MkDir on backend wasn't successful";
					Lwt.fail Connection_pool.NotAvailable

let rec backend_checkdir metadata backend_id dbd =
	if metadata.Metadata_S.fullname = "/" then
		Lwt.return "/"
	else
		lwt reg_backend =
			try
				let r = RegBackend.rw_of_metadata_and_backend ~metadata ~backend:backend_id dbd in
				(* directory exists, just return *)
				Lwt.return r
			with
				| Not_found ->
					(* directory have to be created *)
					let parent_meta = get_parent_meta (metadata.Metadata_S.fullname) dbd in
					lwt parent_path =
						if parent_meta.Metadata_S.fullname = "/" then
							Lwt.return "/"
							(* OK, root always exist *)
						else
						begin
							(* parent isn't root, check it recursively *)
							backend_checkdir parent_meta backend_id dbd
							(* all parents exist, it's time to create directory and return *)
						end
					in
					let dirname = Filename.basename metadata.Metadata_S.fullname in
					backend_mkdir dirname parent_path metadata backend_id dbd
		in
		Lwt.return reg_backend.RegBackend_S.path


