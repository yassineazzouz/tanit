
class ExecutionState:
    SUBMITTED = 1
    SCHEDULED = 2
    DISPATCHED = 3
    RUNNING = 4
    FINISHED = 5
    FAILED = 6

    _VALUES_TO_NAMES = {
      1: "SUBMITTED",
      2: "SCHEDULED",
      3: "DISPATCHED",
      4: "RUNNING",
      5: "FINISHED",
      6: "FAILED",
     }

    _NAMES_TO_VALUES = {
      "SUBMITTED": 1,
      "SCHEDULED": 2,
      "DISPATCHED": 3,
      "RUNNING": 4,
      "FINISHED": 5,
      "FAILED": 6,
    }
