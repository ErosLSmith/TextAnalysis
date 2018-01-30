def fname(arg: str, arg2: list)-> str:
    """
    I am a docsting
    Args:
        arg: a string.
        arg2: a list of stuff.
    Returns:
        string: Same as arg entered
    """
    print(arg)
    return arg


fname("hello", {})
fname(9, [])

print(fname.__annotations__)
