namespace py worker.service

struct WorkerStatus {
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

struct FileSystem {
    1: required string name,
    2: required map<string,string> parameters
}

service WorkerService{

    void submit(1:Task task),
    
    WorkerStatus worker_status(),

    void register_filesystem(1:FileSystem filesystem),
    
}
