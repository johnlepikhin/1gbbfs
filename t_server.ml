
type state =
	| Alive
	| DeadFrom of float

type addr = {
	inet_addr : Unix.inet_addr;
	port : int;
	mutable state : state;
}

type server = {
	id : int64;
	name : string;
	addr : addr;
	db_prio_read : int;
	db_prio_write : int;
	mutable local_prio : int;
	storage : string;
}
