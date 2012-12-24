open Ocamlbuild_plugin

let dispatcher = function
	| After_rules ->

		dep  ["link"; "ocaml"; "use_lbigarray"] ["l_bigarray_stubs.o"];
(*
		flag ["ocaml"; "link"; "static"] ["-ccopt"; "-static"];
*)
		rule "Clear build info" ~prod:"buildinfo.clear" ~deps:[] (fun env _build ->
			Cmd (S[A"rm"; A"buildtime.ml"; A"buildcounter.ml"]);
		);

		rule "Make build info" ~prods:["buildinfo.make"; "buildcounter.ml"] ~deps:[] (fun env _build ->
			Seq [
				Cmd (S[A"sh"; A"-c"; A"date +'let v = \"%F %X\"' >buildtime.ml"]);
				Cmd (S[A"sh"; A"-c"; A"i=$((`cat ../buildcounter`+1)); echo $i>../buildcounter; echo \"let v = $i\" >buildcounter.ml"]);
			]
		);

		rule "Make" ~prod:"make" ~deps:["bfs_client.native"; "bfs_server.native"] (fun _ _ -> Nop);

		()
	| _ -> ()

let () =
	dispatch dispatcher
