from functools import wraps
from threading import Thread


def run_async(func):
    @wraps(func)
    def async_func(*args, **kwargs):
        func_hl = Thread(
            target=func,
            args=args,
            kwargs=kwargs,
            name="Thread_{}".format(func.__name__),
        )
        func_hl.start()
        return func_hl

    return async_func


def str2bool(s):
    if s.lower() == "true":
        return True
    elif s.lower() == "false":
        return False
    else:
        raise BooleanConversionException("can not convert %s to boolean" % s)


class BooleanConversionException(Exception):
    pass
