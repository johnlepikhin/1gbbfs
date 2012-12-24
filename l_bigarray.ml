

type a = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

(* read fd buffer offset length -> bytes read *)
external read: Unix.file_descr -> a -> int -> int -> int = "caml_read"

(* write fd buffer offset length -> bytes written *)
external write: Unix.file_descr -> a -> int -> int -> int = "caml_write"
