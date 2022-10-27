import importlib
import inspect

from cloudpickle.cloudpickle import _extract_code_globals


def _list_globals(func):
    f_globals = {}
    if hasattr(func, '__code__'):
        for var in _extract_code_globals(func.__code__):
            if var in func.__globals__:
                f_globals[var] = func.__globals__[var]
    return f_globals


def _list_locals(func):
    class CollectLocals(dict):
        def __getitem__(self, var):
            f_locals[var] = obj = main.__dict__[var]
            return obj

    f_locals = {}
    main = importlib.import_module('__main__')
    exec(inspect.getsource(func), CollectLocals())
    return f_locals


def _build_import(name, obj):
    if inspect.ismodule(obj):
        mod_name, mod_alias = obj.__name__, name
        if mod_name != mod_alias:
            return f'import {mod_name} as {mod_alias}'
        else:
            return f'import {mod_name}'
    elif inspect.isclass(obj) or inspect.ismethod(obj) or inspect.isfunction(obj):
        mod = inspect.getmodule(obj)
        if mod:
            mod_name = mod.__name__
            obj_name, obj_alias = obj.__name__, name
            if obj_name != obj_alias:
                return f'from {mod_name} import {obj_name} as {obj_alias}'
            else:
                return f'from {mod_name} import {obj_name}'
    else:
        # try heuristics and walk down the path
        mod = inspect.getmodule(obj)
        if mod:
            mod_name = mod.__name__
            mod_path = mod_name.split('.')
            for i in range(len(mod_path)):
                par_name = '.'.join(mod_path[:i+1])
                par = importlib.import_module(par_name)
                if hasattr(par, name):
                    return f'from {par_name} import {name}'
    raise TypeError(f'"{name}" of type {type(obj)} is not importable')


def _build_code(func):
    imports = set()
    sources = []
    for vars in (_list_globals(func), _list_locals(func)):
        for name, obj in vars.items():
            if obj == func:
                continue
            mod = inspect.getmodule(obj)
            if mod:
                if inspect.isfunction(obj) and mod.__name__ == '__main__':
                    imports_, sources_ = _build_code(obj)
                    imports.update(imports_)
                    sources.extend(sources_)
                else:
                    imports.add(_build_import(name, obj))
            else:
                raise TypeError(f'"{name}" of type {type(obj)} is ambiguous')
                # sources.append(f'{name} = {obj}\n')
    sources.append(inspect.getsource(func))
    return imports, sources


def find_code(func):
    imports, sources = _build_code(func)
    code = '\n'.join(sorted(imports))
    if code and sources:
        code += '\n\n\n'
    return code + '\n\n'.join(sources)
