class Job(object):
    def __init__(self, etype, params):
        self.etype = etype
        self.params = params


class JobStatus(object):
    def __init__(
        self, jid, state, submission_time, start_time, finish_time, execution_time
    ):
        self.jid = jid
        self.state = state
        self.submission_time = submission_time
        self.start_time = start_time
        self.finish_time = finish_time
        self.execution_time = execution_time

    def __str__(self):
        return (
            "JobStatus {"
            "id: %s,"
            "state: %s,"
            "submission_time: %s,"
            "start_time: %s,"
            "finish_time: %s,"
            "execution_time: %s"
            "}"
            % (
                self.jid,
                self.state,
                self.submission_time,
                self.start_time,
                self.finish_time,
                self.execution_time,
            )
        )
