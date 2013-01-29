
open Dbt

let create_missing_backends ?parent_meta ~required_distribution ~meta ~meta_id ~existing_backends_ids dbd =
	let open Metadata_S in
	let parent_meta =
		match parent_meta with
			| None -> Op_common.get_parent_meta meta.fullname dbd
			| Some parent_meta -> parent_meta
	in

	let backends = Backend.get_best_for_create required_distribution in
	lwt () = Lwt_list.iter_p (fun backend ->
		if List.mem backend.T_server.id existing_backends_ids then
			Lwt.return ()
		else
			lwt parent_backend_path = Op_common.backend_checkdir parent_meta backend.T_server.id dbd in
			let ppath = Op_common.add_slash parent_backend_path in
			let basename = Filename.basename meta.fullname in
			let rec try_prefix prefix =
				try
					let backend_path = ppath ^ prefix ^ basename in
					let regbackend =  RegBackend.create ~metadata:(Some meta_id) ~backend:backend.T_server.id backend_path in
					let (_ : int64) = RegBackend.insert dbd regbackend in
					Lwt.return ()
				with
					| _ ->
						let prefix = Op_common.generate_prefix () in
						try_prefix prefix
			in
			try_prefix ""
	) backends in
	Lwt.return ()

let op_create path mode dbd =
	let parent_meta = Op_common.get_parent_meta path dbd in

	let open Metadata_S in
	let meta = Metadata.regular mode path in
	let meta = { meta with deepness = parent_meta.deepness + 1; required_distribution = parent_meta.required_distribution } in
	let meta_id = Metadata.insert dbd meta in
	create_missing_backends ~parent_meta ~required_distribution:parent_meta.required_distribution ~meta ~meta_id ~existing_backends_ids:[] dbd


