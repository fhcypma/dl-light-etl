import functools


def foldl(func, acc, xs):
    """Fold left function"""
    return functools.reduce(func, xs, acc)
