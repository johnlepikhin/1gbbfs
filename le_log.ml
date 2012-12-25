
let mutex = Mutex.create ()

let log ?arg ~filename ~line s =
(*	ignore (Unix.select [] [] [] 0.1); *)
	let real_print () =
		Mutex.lock mutex;
		Printf.printf "%.6f %s, line %i: %s\n" (Unix.gettimeofday ()) filename line s;
		flush stdout;
		Mutex.unlock mutex
	in
	match arg with
		| None -> real_print ()
		| Some level ->
			if level < 3 then
				real_print ()

let to_readable ?(limit=20) buf =
	let r = Buffer.create (limit*3) in
	for i=0 to (limit-1) do
		let c = buf.{i} in
		Buffer.add_string r (Printf.sprintf " %02x" (Char.code c))
	done;
	Buffer.contents r
