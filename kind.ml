
open Unix

let of_int i =
	match i with
		| 0 -> S_REG
		| 1 -> S_DIR
		| 2 -> S_CHR
		| 3 -> S_BLK
		| 4 -> S_LNK
		| 5 -> S_FIFO
		| 6 -> S_SOCK
		| _ -> raise Not_found

let to_int k =
	match k with
		| S_REG -> 0
		| S_DIR -> 1
		| S_CHR -> 2
		| S_BLK -> 3
		| S_LNK -> 4
		| S_FIFO -> 5
		| S_SOCK -> 6

let dir = to_int S_DIR
