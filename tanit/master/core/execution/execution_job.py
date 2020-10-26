import abc
import logging as lg
import os
import os.path as osp
from datetime import datetime
from hashlib import md5
from threading import Lock

import six

from ....common.core.exception import TanitError
from ....common.model.execution_type import ExecutionType
from ....common.utils import glob
from ....common.utils.utils import str2bool
from .execution_state import ExecutionState

_logger = lg.getLogger(__name__)


class TaskExecution(object):
    """TaskExecution represent the execution flow of a task in the master."""

    def __init__(self, tid, etype, params, job):
        self.state = ExecutionState.SUBMITTED

        self.tid = tid
        self.etype = etype
        self.params = params
        self.job = job
        self.attempts = 1
        self.worker = None

    def on_schedule(self):
        if self.state not in [ExecutionState.SUBMITTED, ExecutionState.SCHEDULED]:
            raise IllegalStateTransitionException(
                "Can not transition from state %s to state %s",
                ExecutionState._VALUES_TO_NAMES[self.state],
                ExecutionState._VALUES_TO_NAMES[ExecutionState.SCHEDULED],
            )
        self.state = ExecutionState.SCHEDULED
        self.job.on_task_schedule(self.tid)

    def on_dispatch(self, worker=None):
        if self.state not in [ExecutionState.DISPATCHED, ExecutionState.SCHEDULED]:
            raise IllegalStateTransitionException(
                "Can not transition from state %s to state %s",
                ExecutionState._VALUES_TO_NAMES[self.state],
                ExecutionState._VALUES_TO_NAMES[ExecutionState.DISPATCHED],
            )
        self.state = ExecutionState.DISPATCHED
        self.worker = worker
        self.job.on_task_dispatch(self.tid)

    def on_start(self):
        if self.state not in [
            ExecutionState.RUNNING,
            ExecutionState.DISPATCHED,
            ExecutionState.FAILED,
        ]:  # NOQA
            raise IllegalStateTransitionException(
                "Can not transition from state %s to state %s",
                ExecutionState._VALUES_TO_NAMES[self.state],
                ExecutionState._VALUES_TO_NAMES[ExecutionState.RUNNING],
            )
        self.state = ExecutionState.RUNNING
        self.job.on_task_start(self.tid)

    def on_finish(self):
        if self.state not in [ExecutionState.RUNNING, ExecutionState.FINISHED]:
            raise IllegalStateTransitionException(
                "Can not transition from state %s to state %s",
                ExecutionState._VALUES_TO_NAMES[self.state],
                ExecutionState._VALUES_TO_NAMES[ExecutionState.RUNNING],
            )
        self.state = ExecutionState.FINISHED
        self.job.on_task_finish(self.tid)

    def on_fail(self):
        if self.state not in [ExecutionState.RUNNING, ExecutionState.FAILED]:
            raise IllegalStateTransitionException(
                "Can not transition from state %s to state %s",
                ExecutionState._VALUES_TO_NAMES[self.state],
                ExecutionState._VALUES_TO_NAMES[ExecutionState.RUNNING],
            )
        self.state = ExecutionState.FAILED
        self.job.on_task_fail(self.tid)

    def on_reset(self):
        self.job.on_task_reset(self.tid)

    def on_retry(self):
        self.attempts += 1
        self.job.on_task_reset(self.tid)

    def _reset(self):
        self.state = ExecutionState.SUBMITTED
        self.worker = None


class IllegalStateTransitionException(Exception):
    pass


@six.add_metaclass(abc.ABCMeta)
class JobExecution(object):
    """
    JobExecution represent the execution flow of a job in the master.

    The job state diagram is as follow:
    ----------------------------------------------------------------------
    |   SUBMITTED --> SCHEDULED --> DISPATCHED --> RUNNING --> FINISHED  |
    |                                                |                   |
    |                                                |                   |
    |                                              FAILED                |
    ---------------------------------------------------------------------|
    """

    def __init__(self, params):

        self.state = ExecutionState.SUBMITTED

        self.submission_time = datetime.now()
        self.schedule_time = None
        self.dispatch_time = None
        self.start_time = None
        self.finish_time = None
        self.execution_time_s = -1

        self.tasks = {}

        self.scheduled_tasks = 0
        self.dispatched_tasks = 0
        self.started_tasks = 0
        self.finished_tasks = 0
        self.failed_tasks = 0

        self.jlock = Lock()

        self.initialize(params)

    @abc.abstractmethod
    def initialize(self, params):
        return

    @abc.abstractmethod
    def setup(self):
        return

    def add_task(self, tid, params):
        self.tasks[tid] = TaskExecution(tid, self.etype, params, self)

    def get_task(self, tid):
        if tid in self.tasks:
            return self.tasks[tid]
        else:
            return None

    def get_tasks(self):
        return list(self.tasks.values())

    def get_state(self):
        return self.state

    def on_task_schedule(self, tid):
        with self.jlock:
            self.scheduled_tasks += 1
            # Only the first task transition the state of the job
            if self.state == ExecutionState.SUBMITTED and self.scheduled_tasks == 1:
                _logger.info("Job [ %s ] execution scheduled.", self.jid)
                self.schedule_time = datetime.now()
                self.state = ExecutionState.SCHEDULED

    def on_task_dispatch(self, tid):
        with self.jlock:
            self.dispatched_tasks += 1
            # Only the first task transition the state of the job
            if self.state == ExecutionState.SCHEDULED and self.dispatched_tasks == 1:
                _logger.info("Job [ %s ] execution dispatched.", self.jid)
                self.dispatch_time = datetime.now()
                self.state = ExecutionState.DISPATCHED

    def on_task_start(self, tid):
        with self.jlock:
            self.started_tasks += 1
            # Only the first task transition the state of the job
            if self.state == ExecutionState.DISPATCHED and self.started_tasks == 1:
                _logger.info("Job [ %s ] execution started.", self.jid)
                self.start_time = datetime.now()
                self.state = ExecutionState.RUNNING

    def on_task_finish(self, tid):
        with self.jlock:
            self.finished_tasks += 1
            # All tasks need to finish (successfully)
            # to consider the job finished
            if self.finished_tasks == len(self.tasks):
                _logger.info("Job [ %s ] execution finished.", self.jid)
                self.state = ExecutionState.FINISHED
                self.finish_time = datetime.now()
                self.execution_time_s = int(
                    (self.finish_time - self.start_time).total_seconds()
                )

    def on_task_fail(self, tid):
        with self.jlock:
            self.failed_tasks += 1
            if self.state != ExecutionState.FAILED:
                self.state = ExecutionState.FAILED
                _logger.info("Job [ %s ] execution failed.", self.jid)

    def on_task_reset(self, tid):
        task = self.tasks[tid]
        with self.jlock:
            if task.state == ExecutionState.SCHEDULED:
                self.scheduled_tasks -= 1
            elif task.state == ExecutionState.DISPATCHED:
                self.scheduled_tasks -= 1
                self.dispatched_tasks -= 1
            elif task.state == ExecutionState.RUNNING:
                self.scheduled_tasks -= 1
                self.dispatched_tasks -= 1
                self.started_tasks -= 1
            elif task.state == ExecutionState.FINISHED:
                self.scheduled_tasks -= 1
                self.dispatched_tasks -= 1
                self.started_tasks -= 1
                self.finished_tasks -= 1
                if self.state == ExecutionState.FINISHED:
                    self.state = ExecutionState.RUNNING
            elif task.state == ExecutionState.FAILED:
                self.scheduled_tasks -= 1
                self.dispatched_tasks -= 1
                self.started_tasks -= 1
                if self.state == ExecutionState.FAILED and self.failed_tasks == 1:
                    self.state = ExecutionState.RUNNING
                self.failed_tasks -= 1

            task._reset()


class UploadJobExecution(JobExecution):
    def initialize(self, params):
        raise NotImplementedError

    def setup(self):
        raise NotImplementedError


class MockJobExecution(JobExecution):
    def initialize(self, params):
        try:
            self.num_tasks = int(params["num_tasks"]) if "num_tasks" in params else 2
            self.num_failures = (
                int(params["num_failures"]) if "num_failures" in params else 0
            )
            self.sleep = float(params["sleep"]) if "sleep" in params else 2.0
        except Exception as e:
            raise JobInitializationException("Failed to parse job parameters.", e)

        if self.num_failures > self.num_tasks:
            self.num_failures = self.num_tasks

        self.jid = "mock-job-%s" % (str(datetime.now().strftime("%d%m%Y%H%M%S")))

        self.etype = ExecutionType.MOCK

    def setup(self):
        for i in range(self.num_failures):
            self.add_task(
                "{}-task-{}".format(self.jid, i),
                {"fail": "True", "sleep": str(self.sleep)},
            )

        for i in range(self.num_tasks - self.num_failures):
            self.add_task(
                "{}-task-{}".format(self.jid, i + self.num_failures),
                {"fail": "False", "sleep": str(self.sleep)},
            )


class CopyJobExecution(JobExecution):
    def initialize(self, params):

        if "src" in params:
            self.src = params["src"]
        else:
            raise JobInitializationException(
                "missing required copy job parameter 'src'"
            )

        if "dst" in params:
            self.dst = params["dst"]
        else:
            raise JobInitializationException(
                "missing required copy job parameter 'dst'"
            )

        if "src_path" in params:
            self.src_path = params["src_path"]
        else:
            raise JobInitializationException(
                "missing required copy job parameter 'src_path'"
            )

        if "dest_path" in params:
            self.dest_path = params["dest_path"]
        else:
            raise JobInitializationException(
                "missing required copy job parameter 'dest_path'"
            )

        self.include_pattern = (
            params["include_pattern"] if "include_pattern" in params else "*"
        )
        self.min_size = int(params["min_size"]) if "min_size" in params else 0
        self.overwrite = (
            str2bool(params["overwrite"]) if "overwrite" in params else True
        )
        self.force = str2bool(params["force"]) if "force" in params else True
        self.checksum = str2bool(params["checksum"]) if "checksum" in params else True
        self.files_only = (
            str2bool(params["files_only"]) if "files_only" in params else True
        )
        self.chunk_size = int(params["chunk_size"]) if "chunk_size" in params else 65536
        self.buffer_size = (
            int(params["buffer_size"]) if "buffer_size" in params else 65536
        )

        self.jid = "job-%s-%s" % (
            md5(
                "{}-{}-{}-{}".format(
                    self.src, self.src_path, self.dst, self.dest_path
                ).encode("utf-8")
            ).hexdigest(),
            str(datetime.now().strftime("%d%m%Y%H%M%S")),
        )

        self.etype = ExecutionType.COPY

    def setup(self):

        from ....filesystem.filesystem_factory import FileSystemFactory

        src = FileSystemFactory.getInstance().get_filesystem(self.src)
        dst = FileSystemFactory.getInstance().get_filesystem(self.dst)

        src_path = self.src_path
        dst_path = self.dest_path
        overwrite = self.overwrite

        # Normalise src and dst paths
        src_path = src.resolve_path(src_path)

        if str(dst_path).endswith("/"):
            dst_path = dst.resolve_path(dst_path)
            dst_name = ""
        else:
            r_path = dst.resolve_path(dst_path)
            dst_path = os.path.dirname(r_path)
            dst_name = os.path.basename(r_path)

        # First, resolve the list of src files/directories to be copied
        copies = [copy_file for copy_file in glob.glob(src, src_path)]

        # need to develop a proper pattern based access function
        if len(copies) == 0:
            raise TanitError(
                "Cloud not resolve source path %s : "
                + "either it does not exist or can not access it.",
                src_path,
            )

        tuples = []
        for copy in copies:
            copy_tuple = dict()

            # filename = osp.basename(copy)
            # dst_base_path =  osp.join( dst_path, filename )
            status = dst.status(dst_path, strict=False)
            # statuses = [status for _, status in dst.list(dst_base_path)]
            if status is None:
                # Remote path doesn't exist.
                raise TanitError("Base directory of %r does not exist.", dst_path)
            else:
                # Remote path exists.
                if status["type"] == "FILE":
                    # Remote path exists and is a normal file.
                    if not overwrite:
                        raise TanitError(
                            "Destination path %r already exists.", dst_path
                        )
                    # the file is going to be deleted and the destination
                    # is going to be created with the same name
                    dst_base_path = dst_path
                else:
                    # Remote path exists and is a directory.
                    dst_name = osp.basename(copy) if dst_name == "" else dst_name
                    status = dst.status(osp.join(dst_path, dst_name), strict=False)
                    if status is None:
                        # destination does not exist, great !
                        dst_base_path = osp.join(dst_path, dst_name)
                        pass
                    else:
                        # destination exists
                        dst_base_path = osp.join(dst_path, dst_name)
                        if not overwrite:
                            raise TanitError(
                                "Destination path %r already exists.", dst_base_path
                            )

                copy_tuple = dict({"src_path": copy, "dst_path": dst_base_path})
            tuples.append(copy_tuple)

        # This is a workaround for a Bug when copying files using a pattern
        # it may happen that files can have the same name:
        # ex : /home/user/test/*/*.py may result in duplicate files
        for i in range(0, len(tuples)):
            for x in range(i + 1, len(tuples)):
                if tuples[i]["dst_path"] == tuples[x]["dst_path"]:
                    raise TanitError(
                        "Conflicting files %r and %r : can't copy both files to %r"
                        % (  # NOQA
                            tuples[i]["src_path"],
                            tuples[x]["src_path"],
                            tuples[i]["dst_path"],
                        )
                    )

        for copy_tuple in tuples:
            # Then we figure out which files we need to copy, and where.
            src_paths = list(src.walk(copy_tuple["src_path"]))
            if not src_paths:
                # This is a single file.
                src_fpaths = [copy_tuple["src_path"]]
            else:
                src_fpaths = [
                    osp.join(dpath, fname)
                    for dpath, _, fnames in src_paths
                    for fname in fnames
                ]

            offset = len(copy_tuple["src_path"].rstrip(os.sep)) + len(os.sep)

            i = 1
            for fpath in src_fpaths:
                self.add_task(
                    "{}-task-{}".format(self.jid, i),
                    params={
                        "src": str(self.src),
                        "dst": str(self.dst),
                        "src_path": str(fpath),
                        "dest_path": str(
                            osp.join(
                                copy_tuple["dst_path"],
                                fpath[offset:].replace(os.sep, "/"),
                            ).rstrip(os.sep)
                        ),
                        "overwrite": str(self.overwrite),
                        "force": str(self.force),
                        "checksum": str(self.checksum),
                        "chunk_size": str(self.chunk_size),
                        "buffer_size": str(self.buffer_size),
                    },
                )
                i += 1


class JobInitializationException(Exception):
    pass
