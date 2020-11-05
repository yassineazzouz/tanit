include "model.thrift"

namespace py worker.service

service WorkerService{

    void submit(1:model.Task task),
    
    model.WorkerExecutionStatus worker_status(),

    void register_filesystem(1:model.FileSystem filesystem),
    
}
