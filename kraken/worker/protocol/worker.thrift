namespace py thrift

enum TaskState {
  SUBMITTED = 1,
  RUNNING = 2,
  FINISHED = 3,
  FAILED = 4,
}

struct WorkerStatus {
  	1: string wid,
  	2: i32 running,
  	3: i32 pending,
  	4: i32 available,
}

struct Task {
  1: required string tid,
  2: required string src,
  3: required string dest,
  4: required string src_path,
  5: required string dest_path,
  6: optional string include_pattern = "*",
  7: optional i64 min_size = 0,
  8: optional bool preserve = true,
  9: optional bool force = true,
  10: optional bool checksum = false,
  11: optional bool files_only = false,
  12: optional i64 part_size = 65536,
  13: optional i64 buffer_size = 65536,
}

service WorkerService{

    void submit(1:Task task),
    
    WorkerStatus worker_status(),
    
}
