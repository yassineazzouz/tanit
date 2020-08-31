namespace py thrift

struct Worker {
  	1: required string wid,
  	2: optional string address,
  	3: optional i32 port,
}

service WorkerMasterService{

    void register_worker(1:Worker worker),
    
    void send_heartbeat(1:Worker worker),
}