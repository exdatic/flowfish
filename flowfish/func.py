import inspect
from inspect import Parameter, Signature
from types import ModuleType
from typing import Optional, get_type_hints, Any, Callable, Dict, List, Set, Tuple

from flowfish.code import find_code
from flowfish.logger import logger
from flowfish.utils import find_obj


def _find_func(name: str) -> Callable:
    """find function"""
    try:
        obj = find_obj(name)
        if not callable(obj):
            raise ValueError(f'Not a callable: {obj}')
        return obj

    except Exception:
        raise TypeError(f'Function not found: {name}')


def _func_pars(sign: Signature) -> Dict[str, Parameter]:
    """get params (exclude kwargs)"""
    return dict((k, p) for k, p in sign.parameters.items()
                if p.kind is not p.VAR_KEYWORD)


def _func_defs(sign: Signature) -> Dict[str, Any]:
    """get default args"""
    return dict((k, p.default) for k, p in sign.parameters.items()
                if p.default is not p.empty)


def _split_args(sign: Signature, *args, **kwargs) -> Tuple[Dict, Dict, Dict, List]:
    """split given args and kwargs in positional, variadic and keyword arguments"""
    pos_args = dict()
    var_args = dict()
    key_args = dict()
    missing = list()

    params = sign.parameters
    param_list = list(params.values())
    param_idcs = dict((p, i) for i, p in enumerate(params))

    # read from *args first to ensure position
    pos_params = list(filter(lambda p: p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY),
                             params.values()))

    for i, v in enumerate(args):
        if i < len(pos_params):
            pos_args[param_list[i].name] = v
        else:
            var_args[param_list[i].name] = args[i:]
            break

    # read from **kwargs last
    for k, v in sorted(kwargs.items(), key=lambda i: param_idcs.get(i[0], -1)):
        if k in params:
            p = params[k]
            if p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY):
                # do not override pos_args if already set from *args
                if k not in pos_args:
                    pos_args[k] = v
            elif p.kind is p.VAR_POSITIONAL:
                # do not override var_args if already set from *args
                if k not in var_args:
                    if not isinstance(v, (list, tuple)):
                        raise TypeError(f'Invalid varargs: {type(v)} (must be list)')
                    var_args[k] = v
            else:
                key_args[k] = v
        else:
            key_args[k] = v

    # final check for missing args
    for k, p in params.items():
        if p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY):
            if k not in pos_args:
                missing.append(k)
        elif p.kind is p.KEYWORD_ONLY:
            if p.default is p.empty and k not in key_args:
                missing.append(k)

    return pos_args, var_args, key_args, missing


def _fake_sign(sign: Signature, args: Dict) -> Signature:
    """create synthetic signature for given args"""
    args = dict(args)  # create mutable copy

    params = []
    for p in sign.parameters.values():
        name = p.name
        kind = p.kind
        if kind == Parameter.VAR_KEYWORD:
            # insert remaining args before **kwargs
            for key in list(args.keys()):
                default = args.pop(key)
                params.append(Parameter(key, Parameter.KEYWORD_ONLY, default=default))
        else:
            default = args.pop(name, p.default)
            # variadic positional parameters cannot have default values
            if kind is p.VAR_POSITIONAL and default is not p.empty:
                kind = p.POSITIONAL_OR_KEYWORD
            p = Parameter(name, kind, default=default, annotation=p.annotation)
        params.append(p)

    # do not validate, it is for synthetic purposes
    return Signature(params, return_annotation=sign.return_annotation,
                     **{'__validate_parameters__': False})


class Regenerator:

    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        return self

    def __iter__(self):
        yield from self.func(*self.args, **self.kwargs)


class AsyncRegenerator:

    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        return self

    async def __aiter__(self):
        async for i in self.func(*self.args, **self.kwargs):
            yield i


class Func:

    func: Callable
    defs: Dict[str, Any]
    pars: Dict[str, Parameter]
    sign: Optional[Signature]

    def __init__(self, name: str, func: Callable, code: Optional[str] = None):
        self.name = name
        self.func = func
        self.code = code
        try:
            self.sign = inspect.signature(func)
            self.defs = _func_defs(self.sign)
            self.pars = _func_pars(self.sign)
        except ValueError:
            # signature not found (e.g. builtins, cython)
            self.sign = None
            self.defs = dict()
            self.pars = dict()
        self.typs = get_type_hints(func)

    def call(self, *args, **kwargs) -> Any:
        if self.sign:
            pos_args, var_args, key_args, missing = _split_args(self.sign, *args, **kwargs)
        else:
            pos_args, var_args, key_args, missing = dict(), {'*': args}, dict(kwargs), list()

        if missing:
            raise TypeError(f'{self.name}() is missing arguments: {missing}')

        if '*' in key_args:
            var_args = {'*': key_args.pop('*')}

        # try to coerce values to match type hint
        def coerce(k, v):
            if k in self.typs:
                typ = self.typs[k]
                # create "pydantic model" from dict
                if type(v) is dict and hasattr(typ, 'parse_obj'):
                    parse_obj = getattr(typ, 'parse_obj')
                    if inspect.ismethod(parse_obj):
                        return parse_obj(v)
                if type(v) is list and typ is Set:
                    return set(v)
                if type(v) is list and typ is Tuple:
                    return tuple(v)
            return v

        pos_args = dict((k, coerce(k, v)) for k, v in pos_args.items())
        key_args = dict((k, coerce(k, v)) for k, v in key_args.items())

        # call function
        func = self.func

        # make generator functions reiterable
        if inspect.isgeneratorfunction(func):
            func = Regenerator(func)
        elif inspect.isasyncgenfunction(func):
            func = AsyncRegenerator(func)

        if var_args:
            var_vals = next(iter(var_args.values()))
            return func(*pos_args.values(), *var_vals, **key_args)
        else:
            return func(*pos_args.values(), **key_args)

    def call_args(self, *args, **kwargs) -> Dict[str, Any]:
        if self.sign:
            pos_args, var_args, key_args, _ = _split_args(self.sign, *args, **kwargs)
        else:
            pos_args, var_args, key_args, _ = dict(), {'*': args}, dict(kwargs), list()

        return {**pos_args, **var_args, **key_args}

    def fake_sign(self, args: Dict) -> Optional[Signature]:
        return _fake_sign(self.sign, args) if self.sign else None

    @classmethod
    def find(cls, name: str, code: Optional[str] = None) -> 'Func':
        if code:
            mod = ModuleType('__main__')
            exec(code, mod.__dict__)
            func = getattr(mod, name)
            return cls(name, func, code)
        else:
            func = _find_func(name)
            if func.__module__ == '__main__':
                try:
                    code = find_code(func)
                    return cls(name, func, code)
                except Exception as e:
                    logger.warning(f'code extraction failed for "{name}": {e!r}')
            return cls(name, func)
