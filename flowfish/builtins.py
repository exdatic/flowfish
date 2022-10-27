import builtins
import inspect
import os
from typing import Union

import loguru

from flowfish.exec import subprocess, Buffer
from flowfish.utils import find_obj


_MAP_BUILTINS = dict((k, getattr(builtins, k)) for k in (
    'abs', 'all', 'any', 'ascii', 'bin', 'bool', 'bytearray', 'bytes', 'chr',
    'complex', 'dict', 'divmod', 'enumerate', 'filter', 'float', 'hex', 'int',
    'iter', 'len', 'list', 'map', 'max', 'min', 'next', 'oct', 'ord', 'pow',
    'range', 'reversed', 'round', 'set', 'slice', 'sorted', 'str', 'sum',
    'tuple', 'zip'))


def get(input):
    return find_obj(input)


def map_simpleeval(input, value='input', **kwargs):
    """map function"""
    names, funcs = dict(), dict(_MAP_BUILTINS)
    funcs['get'] = find_obj

    for k, v in {'input': input, **kwargs}.items():
        if inspect.isfunction(v) or inspect.ismethod(v) or inspect.isroutine(v):
            funcs[k] = v
        else:
            names[k] = v

    import simpleeval
    # increase simpleeval limits
    simpleeval.MAX_COMPREHENSION_LENGTH = 0x100000000
    expr = simpleeval.EvalWithCompoundTypes(names=names, functions=funcs)
    return expr.eval(value)


def run(_cmd, _args, _shell=False, stdin=None, stdout=None, stderr=None,
        _capture: Union[int, bool] = False, _logger=None, **kwargs):
    """run external command"""
    cmd = [_cmd]

    missing_args = []
    for arg in _args:
        if isinstance(arg, dict):
            for k, v in arg.items():
                if k not in kwargs:
                    missing_args.append(k)
                    continue
                value = kwargs.get(k, None)
                if value is not None and value is not False:
                    if isinstance(v, (tuple, list)):
                        cmd.extend(str(value) if v == '.' else str(v) for v in v)
                    else:
                        cmd.append(str(value) if v == '.' else str(v))
        else:
            cmd.append(str(arg))

    if missing_args:
        raise TypeError(f'"{_cmd}" is missing arguments: {missing_args}')

    # open file streams
    if stdin and not os.stat(stdin).st_size:
        raise ValueError('stdin file may not be empty')

    stdin_ = open(stdin, 'rb') if stdin else None
    stdout_ = open(stdout, 'wb') if stdout else None
    stderr_ = open(stderr, 'wb') if stderr else None

    if _capture:
        buffer = Buffer(-1 if _capture is True else _capture)
        stdout_ = buffer
        stderr_ = buffer
    else:
        buffer = None

    ret = subprocess(*cmd, stdin=stdin_, stdout=stdout_, stderr=stderr_, shell=_shell)

    if buffer:
        logger = loguru.logger if _logger is None else _logger
        for line in buffer:
            logger.info(line.strip())

    if ret != 0:
        raise Exception(f'cmd failed with {ret}: {cmd}')
    else:
        return 0
