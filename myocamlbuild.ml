open Ocamlbuild_plugin

let getpath pkg =
	let s = Ocamlbuild_pack.My_unix.run_and_read ("ocamlfind query " ^ pkg) in
	String.sub s 0 ((String.length s) - 1)


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

		rule "Original syntax" ~prod:"%.ppo" ~deps:["%.ml"]
			begin
				fun env _build ->
					let lwt_path = getpath "lwt.syntax" in
					let le_path = getpath "pa_le" in
					let ml = env "%.ml" in
					let ppo = env "%.ppo" in
					let cmd = S[A"camlp4o"; A"-I"; A"."; A"-I"; A le_path; A"pr_o.cmo"; A"pa_le.cmo"; A"-I"; A lwt_path; A"lwt-syntax-options.cma"; A"lwt-syntax.cma"; P(ml); A"-o"; P(ppo)] in
					Cmd cmd
			end;

		rule "Show original syntax" ~prod:"%.show.ppo" ~dep:"%.ppo"
			begin
				fun env _build ->
					let ppo = env "%.ppo" in
					let cmd = S[A"cat"; P(ppo)] in
					Cmd cmd
			end;

		()
	| _ -> ()

let () =
	dispatch dispatcher
