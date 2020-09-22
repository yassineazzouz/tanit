import time


def wait_until(condition, timeout, period=0.25, *args, **kwargs):
    mustend = time.time() + timeout
    while time.time() < mustend:
        if condition(*args, **kwargs): return True
        time.sleep(period)
    return False