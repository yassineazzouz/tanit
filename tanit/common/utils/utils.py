def str2bool(s):
    if s.lower() == "true":
        return True
    elif s.lower() == "false":
        return False
    else:
        raise BooleanConversionException("can not convert %s to boolean" % s)


class BooleanConversionException(Exception):
    pass
