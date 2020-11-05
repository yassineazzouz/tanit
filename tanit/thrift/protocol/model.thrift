namespace py common.model

struct FileSystem {
    1: required string name,
    2: required i32 type,
    3: optional string parameters,
}

struct FileSystemMount {
    1: required FileSystem filesystem,
    2: optional list<map<string,string>> mounts
}

struct Worker {
  	1: required string wid,
  	2: optional string address,
  	3: optional i32 port,
}

struct WorkerStats {
  	1: required string wid,
  	2: string state,
  	3: string last_heartbeat,
  	4: i32 running_tasks,
  	5: i32 pending_tasks,
  	6: i32 available_cores
}

struct WorkerExecutionStatus {
  	1: string wid,
  	2: i32 running,
  	3: i32 pending,
  	4: i32 available,
}

struct Task {
  1: required string tid,
  2: required string operation,
  3: required map<string,string> params,
}