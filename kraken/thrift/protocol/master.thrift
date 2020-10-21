namespace py master.service

enum JobState {
  SUBMITTED = 1,
  SCHEDULED = 2,
  DISPATCHED = 3,
  RUNNING = 4,
  FINISHED = 5,
  FAILED = 6,
}

enum JobType {
  COPY = 1,
  UPLOAD = 2,
  MOCK = 3, // for testing
}

struct JobStatus {
  	1: string id,
  	2: JobState state,
  	3: string submission_time,
  	4: string start_time,
  	5: string finish_time,
  	6: i32 execution_time,
}

struct Job {
    1: JobType type,
    2: map<string,string> params,
}

struct Worker {
  	1: required string wid,
  	2: optional string address,
  	3: optional i32 port,
}

struct FileSystem {
    1: required string name,
    2: required map<string,string> parameters
}

exception JobNotFoundException {
  1: string message,
}

service MasterUserService{

    // submit a job 
    string submit_job(1:Job job),
    
    // list jobs
    list<JobStatus> list_jobs(),

    // job status
    JobStatus job_status(1:string jid) throws (1:JobNotFoundException e),
    
}

service MasterWorkerService{

    list<Worker> list_workers(),

    void register_worker(1:Worker worker),
    
    void unregister_worker(1:Worker worker),
    
    void register_heartbeat(1:Worker worker),

    void register_filesystem(1:FileSystem filesystem),
   
    void task_start(1:string tid),
        
    void task_success(1:string tid),
    
    void task_failure(1:string tid),
}
