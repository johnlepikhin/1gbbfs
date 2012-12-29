
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
	free_blocks : int64;
	free_files : int64;
	free_blocks_soft_limit: int64;
	free_blocks_hard_limit: int64;
	free_files_soft_limit: int64;
	free_files_hard_limit: int64;
}
