from collections import namedtuple


def is_non_empty_string(s):
    return isinstance(s, str) and s != ""


def empty_str_err(s):
    return "{} should be a non-empty string".format(s)


def is_positive_int(n):
    return isinstance(n, int) and n > 0


def bad_integer_err(n):
    return "{} should be a positive integer".format(n)


ValidationRule = namedtuple('ValidationRule', ['arg', 'func', 'message'])
