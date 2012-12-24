module M1 = Le_log

open T_communication
open T_connection

exception Timeout
exception InvalidVersion

let global_timeout = 10.

let wait_read ?(timeout=global_timeout) s =
	if not (Thread.wait_timed_read s timeout) then
		raise Timeout
(*
	let (r, _, _) = Thread.select [s] [] [] global_timeout in
	if r = [] then
		raise Timeout
	else
		()
*)

let wait_write s =
	if not (Thread.wait_timed_write s global_timeout) then
		raise Timeout
(*
	let (_, r, _) = Thread.select [] [s] [] global_timeout in
	if r = [] then
		raise Timeout
	else
		()
*)

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

(**********************************************************************************************************)

let send_string t v offset len =
	let rec loop p = function
		| 0 -> ()
		| tl ->
			wait_write t.socket;
			let wr = Unix.write t.socket v p tl in
			if wr = 0 then
				raise Timeout
			else
				loop (p+wr) (tl-wr)
	in
	Unix.setsockopt t.socket Unix.TCP_NODELAY false;
	loop offset len;
	Unix.setsockopt t.socket Unix.TCP_NODELAY true

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
		| 0 -> ()
		| tl ->
			wait_write t.socket;
			let wr = L_bigarray.write t.socket a p tl in
			if wr = 0 then
				raise Timeout
			else
				loop (p+wr) (tl-wr)
	in
	loop offset len

(**********************************************************************************************************)

let read_string ?timeout t len =
	let buf = String.create len in
	let rec loop p = function
		| 0 -> ()
		| tl ->
			wait_read ?timeout t.socket;
			let rd = Unix.read t.socket buf p tl in
			if rd = 0 then
				raise Timeout
			else
				loop (p+rd) (tl-rd)
	in
	loop 0 len;
	buf

let read_bin_int ?timeout t =
	let s = read_string ?timeout t 4 in
	int_of_binstring s

let read_value ?timeout t =
	let len = read_bin_int ?timeout t in
	let encoded = read_string t len in
	let v = Marshal.from_string encoded 0 in
	v

let read_to_bigarray t a offset len =
	let rec loop p = function
		| 0 -> ()
		| tl ->
			wait_read t.socket;
			let rd = L_bigarray.read t.socket a p tl in
			if rd = 0 then
				raise Timeout
			else
				loop (p+rd) (tl-rd)
	in
	loop offset len

(**********************************************************************************************************)

let request t v =
	send_value t v;
	read_value t

let disconnect t =
	begin try Unix.shutdown t.socket Unix.SHUTDOWN_ALL with _ -> () end;
	Unix.close t.socket
(*
	t.alive <- false
*)

let connect addr =
	let socket = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
	Unix.setsockopt socket Unix.TCP_NODELAY true;
	Unix.connect socket (Unix.ADDR_INET (addr.T_server.inet_addr, addr.T_server.port));
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
	let server_version = read_bin_int t in
	debug "Remote server sersion = %i, local version = %i" server_version Buildcounter.v;
	if server_version = Buildcounter.v then
		t
	else
	begin
		send_value t Cmd.Bye;
		disconnect t;
		raise InvalidVersion
	end

let listen addr acceptor =
	let l_socket = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
	Unix.setsockopt l_socket Unix.SO_REUSEADDR true;
	Unix.setsockopt l_socket Unix.TCP_NODELAY true;
	Unix.bind l_socket (Unix.ADDR_INET (addr.T_server.inet_addr, addr.T_server.port));
	Unix.listen l_socket 100;
	let rec loop () =
		let (socket, _) = Unix.accept l_socket in
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
		let (_ : Thread.t) = Thread.create acceptor c in
		loop ()
	in
	loop ()
