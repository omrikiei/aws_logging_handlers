from collections import namedtuple


def is_non_empty_string(s):
    return isinstance(s, str) and s != ""


def empty_str_err(s):
    return "{} should be a non-empty string".format(s)


def is_positive_int(n):
    return isinstance(n, int) and n > 0


def bad_integer_err(n):
    return "{} should be a positive integer".format(n)


def is_boolean(b):
    return isinstance(b, bool)


def bad_type_error(n, t):
    return "{} should be of type {}".format(n, t)


def is_string_func(o):
    return isinstance(o, str) and o or callable(o)


ValidationRule = namedtuple('ValidationRule', ['arg', 'func', 'message'])
