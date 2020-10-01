import time


def wait_until(condition, timeout, period=0.25, *args, **kwargs):
    must_end = time.time() + timeout
    while time.time() < must_end:
        if condition(*args, **kwargs):
            return True
        time.sleep(period)
    return False
