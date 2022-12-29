import functools


def fold_left(iterator, accumulator, operator):
    """Fold left function"""
    return functools.reduce(operator, iterator, accumulator)
