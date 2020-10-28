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

struct WorkerStats {
  	1: string wid,
  	2: string state,
  	3: string last_heartbeat,
  	4: i32 running_tasks,
  	5: i32 pending_tasks,
  	6: i32 available_cores
}

struct FileSystem {
    1: required string name,
    2: required map<string,string> parameters
}

exception JobInitializationException {
  1: string message,
}

exception JobNotFoundException {
  1: string message,
}

service MasterUserService{

    // Manage jobs
    string submit_job(1:Job job) throws (1:JobInitializationException e),

    JobStatus job_status(1:string jid) throws (1:JobNotFoundException e),

    list<JobStatus> list_jobs(),

    // Manage workers
    list<Worker> list_workers(),

    void deactivate_worker(1:string wid),

    void activate_worker(1:string wid),

    WorkerStats worker_stats(1:string wid),
    
}

service MasterWorkerService{

    void register_worker(1:Worker worker),
    
    void unregister_worker(1:Worker worker),
    
    void register_heartbeat(1:Worker worker),

    void register_filesystem(1:FileSystem filesystem),
   
    void task_start(1:string tid),
        
    void task_success(1:string tid),
    
    void task_failure(1:string tid),
}
