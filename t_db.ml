
type dbd = {
	dbd : Mysql.dbd;
	mutex : Mutex.t;
	mutable tr_id : int;
}


