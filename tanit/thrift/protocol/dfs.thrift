namespace py master.dfs

struct FileStatus {
  	1: string fileId,
  	2: optional string length,
  	3: optional string type,
  	4: optional string modificationTime
}

struct FileStatusOrNull {
    1: string path,
  	2: FileStatus status
}

struct FileContent {
  	1: string length,
  	2: string fileCount,
  	3: string directoryCount
}

struct FileContentOrNull {
    1: string path,
  	2: FileContent content
}

exception FileSystemException {
  1: string message,
}

service DistributedFilesystem{
    list<FileStatusOrNull> ls(1:string path, 2:bool status, 3:bool glob) throws (1:FileSystemException e),

    FileStatusOrNull status(1:string path, 2:bool strict) throws (1:FileSystemException e),

    FileContentOrNull content(1:string path, 2:bool strict) throws (1:FileSystemException e),

    void rm(1:string path, 2:bool recursive) throws (1:FileSystemException e),

    void mkdir(1:string path, 2:string permission) throws (1:FileSystemException e),

    void copy(1:string src_path, 2:string  dst_path, 3:bool overwrite, 4:bool force, 5:bool checksum) throws (1:FileSystemException e),

    void move(1:string src_path, 2:string dst_path) throws (1:FileSystemException e),

    string checksum(1:string path, 2:string algorithm) throws (1:FileSystemException e),
}