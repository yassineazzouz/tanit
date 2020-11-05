include "model.thrift"

namespace py master.service

service MasterUserService{

    // Manage workers
    list<model.Worker> list_workers(),

    void deactivate_worker(1:string wid),

    void activate_worker(1:string wid),

    model.WorkerStats worker_stats(1:string wid),

    // Manage filesystems
    void register_filesystem(1:model.FileSystem filesystem),

    list<model.FileSystemMount> list_filesystems(),

    void mount_filesystem(1:string name, 2:string mount_point, 3:string mount_path = ""),

    void umount_filesystem(1:string mount_point),

}

service MasterWorkerService{

    void register_worker(1:model.Worker worker),
    
    void unregister_worker(1:model.Worker worker),
    
    void register_heartbeat(1:model.Worker worker),

    void register_filesystem(1:model.FileSystem filesystem),
   
    void task_start(1:string tid),
        
    void task_success(1:string tid),
    
    void task_failure(1:string tid),
}
