type t = {
	socket : Lwt_unix.file_descr;
(*
	in_channel : in_channel;
	out_channel : out_channel;
	mutable alive : bool;
*)
}

