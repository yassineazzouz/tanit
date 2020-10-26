namespace py worker.filesystem

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

service LocalFilesystem{

    list<FileStatusOrNull> ls(1:string path, 2:bool status, 3:bool glob) throws (1:FileSystemException e),

    FileStatusOrNull status(1:string path, 2:bool strict) throws (1:FileSystemException e),

    FileContentOrNull content(1:string path, 2:bool strict) throws (1:FileSystemException e),

    void rm(1:string path, 2:bool recursive) throws (1:FileSystemException e),

    void copy(1:string src_path, 2:string dst_path) throws (1:FileSystemException e),

    void rename(1:string src_path, 2:string dst_path) throws (1:FileSystemException e),

    void set_owner(1:string path, 2:string owner, 3:string group) throws (1:FileSystemException e),

    void set_permission(1:string path, 2:string permission) throws (1:FileSystemException e),

    void mkdir(1:string path, 2:string permission) throws (1:FileSystemException e),

    # File functions

    string open(1:string path, 2:string mode) throws (1:FileSystemException e),

    void close(1:string filedesc) throws (1:FileSystemException e),

    void flush(1:string filedesc) throws (1:FileSystemException e),

    binary read(1:string filedesc, 2:i32 size) throws (1:FileSystemException e),

    binary readline(1:string filedesc) throws (1:FileSystemException e),

    list<binary> readlines(1:string filedesc, 2:i32 sizehint) throws (1:FileSystemException e),

    i32 write(1:string filedesc, 2:binary data) throws (1:FileSystemException e),

    i32 writelines(1:string filedesc, 2:list<binary> lines) throws (1:FileSystemException e),

    i32 tell(1:string filedesc) throws (1:FileSystemException e),

    i32 seek(1:string filedesc, 2:i32 position) throws (1:FileSystemException e)

    bool readable(1:string filedesc) throws (1:FileSystemException e)

    bool writable(1:string filedesc) throws (1:FileSystemException e)

    bool seekable(1:string filedesc) throws (1:FileSystemException e)
}