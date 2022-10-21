"""Module to define miscellaneous helper methods"""


def print_list(l):
    """Print one list element per line"""
    print('\n'.join(l))


def print_debug(name, val, print_mode=True):
    """Print variable name and value"""
    if print_mode:
        print(f"{name}: {val}")


def partition_list(l, condition):
    """Given a single list, return separate lists of elements that pass or fail a condition"""
    passes = []
    fails = []
    for x in l:
        if condition(x):
            passes.append(x)
        else:
            fails.append(x)
    return passes, fails

def flatten(l):
    """Flatten arbitrarily nested list"""
    # https://stackoverflow.com/questions/2158395/
    flattened_list = []
    def loop(sublist):
        for item in sublist:
            if isinstance(item, list):
                loop(item)
            else:
                flattened_list.append(item)
    loop(l)
    return flattened_list
