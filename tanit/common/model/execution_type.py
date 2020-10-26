class ExecutionType:
    COPY = 1
    UPLOAD = 2
    MOCK = 3

    _VALUES_TO_NAMES = {
        1: "COPY",
        2: "UPLOAD",
        3: "MOCK",
    }

    _NAMES_TO_VALUES = {
        "COPY": 1,
        "UPLOAD": 2,
        "MOCK": 3,
    }
