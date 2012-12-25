module M1 = Le_log

open T_communication
open T_connection

exception Timeout
exception InvalidVersion

let global_timeout = 10.

let binstring_of_int i =
	let s = String.create 4 in
	let b1 = i lsr 24 in
	let i = i - (b1 lsl 24) in
	let b2 = i lsr 16 in
	let i = i - (b2 lsl 16) in
	let b3 = i lsr 8 in
	let b4 = i - (b3 lsl 8) in
	s.[0] <- Char.unsafe_chr b1;
	s.[1] <- Char.unsafe_chr b2;
	s.[2] <- Char.unsafe_chr b3;
	s.[3] <- Char.unsafe_chr b4;
	s

let int_of_binstring s =
	let i = (Char.code s.[3]) in
	let i = i + ((Char.code s.[2]) lsl 8) in
	let i = i + ((Char.code s.[1]) lsl 16) in
	let i = i + ((Char.code s.[0]) lsl 24) in
	i

(* with bugfix *)
let with_timeout t f =
	let end_time = (Unix.gettimeofday ()) +. t in
	let rec loop () =
		try_lwt
			Lwt_unix.with_timeout t f
		with
			| Lwt_unix.Timeout ->
				let now = Unix.gettimeofday () in
				if now < end_time then
					loop ()
				else
					Lwt.fail Lwt_unix.Timeout
	in
	loop ()

(**********************************************************************************************************)

let send_string t v offset len =
	let rec loop p = function
		| 0 -> Lwt.return ()
		| tl ->
			lwt wr = with_timeout global_timeout (fun () -> lwt () = Lwt_unix.wait_write t.socket in Lwt_unix.write t.socket v p tl) in
			if wr = 0 then
			begin
				Lwt.fail Timeout
			end
			else
				loop (p+wr) (tl-wr)
	in
	Lwt_unix.setsockopt t.socket Unix.TCP_NODELAY false;
	lwt () = loop offset len in
	Lwt_unix.setsockopt t.socket Unix.TCP_NODELAY true;
	Lwt.return ()

let send_full_string t s =
	send_string t s 0 (String.length s)

let send_bin_int t v =
	let s = binstring_of_int v in
	send_full_string t s

let send_value t v =
	let encoded = Marshal.to_string v [] in
	let binlen = binstring_of_int (String.length encoded) in
	send_full_string t (binlen ^ encoded)

let rec write_from_bigarray t a offset len =
	let rec loop p = function
		| 0 -> Lwt.return ()
		| tl ->
			lwt () = with_timeout global_timeout (fun () -> Lwt_unix.wait_write t.socket) in
			let wr = L_bigarray.write (Lwt_unix.unix_file_descr t.socket) a p tl in
			if wr = 0 then
			begin
				Lwt.fail Timeout
			end
			else
				loop (p+wr) (tl-wr)
	in
	loop offset len

(**********************************************************************************************************)

let read_string ?timeout t len =
	let buf = String.create len in
	let timeout = match timeout with | None -> global_timeout | Some t -> t in
	let rec loop p = function
		| 0 -> Lwt.return ()
		| tl ->
			lwt rd = with_timeout timeout (fun () -> lwt () = Lwt_unix.wait_read t.socket in Lwt_unix.read t.socket buf p tl) in
			if rd = 0 then
			begin
				Lwt.fail Timeout
			end
			else
				loop (p+rd) (tl-rd)
	in
	lwt () = loop 0 len in
	Lwt.return buf

let read_bin_int ?timeout t =
	lwt s = read_string ?timeout t 4 in
	let r = int_of_binstring s in
	Lwt.return r

let read_value ?timeout t =
	lwt len = read_bin_int ?timeout t in
	lwt encoded = read_string t len in
	let v = Marshal.from_string encoded 0 in
	Lwt.return v

let read_to_bigarray t a offset len =
	let rec loop p = function
		| 0 -> Lwt.return ()
		| tl ->
			lwt () = with_timeout global_timeout (fun () -> Lwt_unix.wait_read t.socket) in
			let rd = L_bigarray.read (Lwt_unix.unix_file_descr t.socket) a p tl in
			if rd = 0 then
			begin
				Lwt.fail Timeout
			end
			else
				loop (p+rd) (tl-rd)
	in
	loop offset len

(**********************************************************************************************************)

let request t v =
	lwt () = send_value t v in
	read_value t

let disconnect t =
	lwt () =
		try_lwt
			Lwt_unix.shutdown t.socket Unix.SHUTDOWN_ALL;
			Lwt.return ()
		with _ -> Lwt.return ()
	in
	Lwt_unix.close t.socket
(*
	t.alive <- false
*)

let connect addr =
	let socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
	Lwt_unix.setsockopt socket Unix.TCP_NODELAY true;
	lwt () = Lwt_unix.connect socket (Unix.ADDR_INET (addr.T_server.inet_addr, addr.T_server.port)) in
(*
	let in_channel = Unix.in_channel_of_descr socket in
	let out_channel = Unix.out_channel_of_descr socket in
*)
	let t = {
		socket;
(*
		in_channel;
		out_channel;
		alive = true;
*)
	} in
	lwt server_version = read_bin_int t in
	debug "Remote server sersion = %i, local version = %i" server_version Buildcounter.v;
	if server_version = Buildcounter.v then
		Lwt.return t
	else
	begin
		lwt () = send_value t Cmd.Bye in
		lwt () = disconnect t in
		Lwt.fail InvalidVersion
	end

let listen addr acceptor =
	let l_socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
	Lwt_unix.setsockopt l_socket Unix.SO_REUSEADDR true;
	Lwt_unix.setsockopt l_socket Unix.TCP_NODELAY true;
	Lwt_unix.bind l_socket (Unix.ADDR_INET (addr.T_server.inet_addr, addr.T_server.port));
	Lwt_unix.listen l_socket 100;
	let rec loop () =
		lwt (socket, _) = Lwt_unix.accept l_socket in
(*
		let in_channel = Unix.in_channel_of_descr socket in
		let out_channel = Unix.out_channel_of_descr socket in
*)
		let c = {
			socket;
(*
			in_channel;
			out_channel;
			alive = true;
*)
		} in
		ignore (acceptor c);
		loop ()
	in
	loop ()
