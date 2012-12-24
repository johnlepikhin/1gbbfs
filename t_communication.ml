
module Cmd = struct
	type t =
		| Bye (* always first and simple *)
		| FOpen of (string * Unix.open_flag list)
		| Read of (int64 * int64 * int) (* fd, offset, size *)
		| Write of (int64 * int64 * int) (* fd, offset, size *)
		| Unlink of string
		| Truncate of (int64 * int64) (* fd, len *)
		| MkDir of string (* path *)
		| RmDir of string (* path *)
		| FClose of int64 (* fd *)
		| Rename of (string * string)


	let to_string = function
		| Bye -> "bye"
		| FOpen _ -> "fopen"
		| Read _ -> "read"
		| Write _ -> "write"
		| Unlink _ -> "unlink"
		| Truncate _ -> "truncate"
		| MkDir _ -> "mkdir"
		| RmDir _ -> "rmdir"
		| FClose _ -> "fclose"
		| Rename _ -> "rename"
end

module R_MkDir = struct
	type t =
		| OK
		| AlreadyExists
end

module Response = struct
	type 'a t =
		| Data of 'a
		| UnixError of Unix.error
		| Error of string

	
end
