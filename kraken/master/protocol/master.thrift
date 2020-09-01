namespace py thrift

enum JobState {
  SUBMITTED = 1,
  RUNNING = 2,
  FINISHED = 3,
  FAILED = 4,
}

struct JobStatus {
  	1: string id,
  	2: JobState state,
  	3: string submission_time,
}

struct JobConf {
  1: required string src,
  2: required string dest,
  3: required string src_path,
  4: required string dest_path,
  5: optional string include_pattern = "*",
  6: optional i64 min_size = 0,
  7: optional bool preserve = true,
  8: optional bool force = true,
  9: optional bool checksum = false,
  10: optional bool files_only = false,
  11: optional i64 part_size = 65536,
  12: optional i64 buffer_size = 65536,
}

struct Worker {
  	1: required string wid,
  	2: optional string address,
  	3: optional i32 port,
}

service MasterClientService{

    // submit a job 
    void submit_job(1:JobConf conf),
    
    // list jobs
    list<JobStatus> list_jobs(),

    // job status
    JobStatus job_status(1:string jid),
    
}

service MasterWorkerService{

    void register_worker(1:Worker worker),
    
    void send_heartbeat(1:Worker worker),
}
