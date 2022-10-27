import asyncio
from collections import defaultdict
import concurrent.futures as cf
import ctypes
import importlib
import os
from pathlib import Path
import pkg_resources
from pkg_resources import DistributionNotFound, VersionConflict
import shutil
from threading import Lock, Thread
from typing import Any, Callable, Dict, List, Optional, Union
import warnings

import murmurhash
from tqdm import tqdm

from flowfish.exec import subprocess


class LoopThread:

    def __init__(self):
        self._lock = Lock()
        self._loop = None

    def loop(self):
        with self._lock:
            if not self._loop:
                self._loop = asyncio.new_event_loop()
                Thread(target=self._loop.run_forever, daemon=True).start()
            return self._loop

    def stop(self):
        with self._lock:
            if self._loop:
                self._loop.call_soon_threadsafe(self._loop.stop)

    def __del__(self):
        self.stop()


def find_obj(name: str) -> Any:
    """find object"""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore")
        segments = name.split('.')

        # foo.bar() or foo.Bar()
        if len(segments) > 1:
            module_name = '.'.join(segments[:-1])
            target_name = segments[-1]
            try:
                module = importlib.import_module(module_name)
                target = getattr(module, target_name)
            except ImportError:
                # foo.Bar.create()
                if len(segments) > 2:
                    module_name = '.'.join(segments[:-2])
                    object_name = segments[-2]
                    target_name = segments[-1]
                    module = importlib.import_module(module_name)
                    objekt = getattr(module, object_name)
                    target = getattr(objekt, target_name)
                else:
                    raise
        else:
            # check main module
            module = importlib.import_module('__main__')
            if hasattr(module, name):
                target = getattr(module, name)
            else:
                # fallback to builtins
                module = importlib.import_module('builtins')
                target = getattr(module, name)

        return target


def sorted_dict(d: Dict):
    return dict(sorted(d.items(), key=lambda i: i[0]))


def copy_props(source: Optional[Dict], target: Dict, prefixes: Optional[List[str]] = None, overwrite=True):
    if source:
        assert isinstance(source, dict), f'expect dict and not {type(source)}'
        assert isinstance(target, dict), f'expect dict and not {type(target)}'
        # [foo, bar] -> foo.bar.
        prefix = '.'.join(prefixes) + '.' if prefixes else None
        for k, v in source.items():
            if not prefix:
                if overwrite or k not in target:
                    target[k] = v
            elif k.startswith(prefix):
                k = k[len(prefix):]
                if overwrite or k not in target:
                    target[k] = v


def fake_hash(obj) -> str:
    """use object identity as fake hash"""
    return format(id(obj) & (1 << 32)-1, 'x')


def hash32(dump: Union[bytes, str]) -> str:
    """Creates a 32-bit hash from bytes or string"""
    return format(murmurhash.hash(dump) & (1 << 32)-1, 'x')


def copy_file(src: Path, dst: Path):
    if not src.is_file():
        raise FileNotFoundError(src)

    if dst.is_file():
        src_size = src.lstat().st_size
        dst_size = src.lstat().st_size
        modified = src_size != dst_size
    else:
        modified = True

    if modified:
        os.makedirs(dst.parent, exist_ok=True)
        try:
            # create hard link
            os.link(src, dst)
            # update modified time
            os.utime(dst)
        except OSError:
            tmp = dst.with_name(dst.name + '.tmp')
            shutil.copy(src, tmp)
            os.rename(tmp, dst)


def resolve_packages(*requirements: str) -> List[str]:
    missing_pkgs = []
    for p in requirements:
        try:
            dist = pkg_resources.get_distribution(p)
        except (DistributionNotFound, VersionConflict):
            dist = None
        # double check if package really exists
        if dist is None or hasattr(dist, 'egg_info') and not os.path.exists(getattr(dist, 'egg_info')):
            missing_pkgs.append(p)
    return missing_pkgs


def pip_install(*requirements: str):
    ret = subprocess('pip', 'install', '-q', *requirements)
    if ret != 0:
        raise Exception(f'pip install for {requirements} failed with {ret}')


def wrap_tqdm(name, generator):
    return (i for i in tqdm(generator, desc=name, unit_scale=True, leave=True))


def pool_run(tasks: Dict[Callable, Optional[Union[Callable, List[Callable]]]]):
    """
    Run tasks in parallel. A task takes the output of its dependencies as input:

        def task(results):
            return process(results)

    Parameters
    ----------
    tasks : dict
        mapping of tasks with their dependencies
    """
    # ensure tasks: Dict[Callable, Union[None, Set[Callable]]]
    tasks = dict((k, set(v) if isinstance(v, (list, tuple, set)) else {v} if v else None)  # type: ignore
                 for k, v in tasks.items())
    tasks.update((v, None) for v in list(filter(None, tasks.values()))
                 for v in v if v not in tasks)  # type: ignore

    futures = dict()
    results = defaultdict(list)

    pool = cf.ThreadPoolExecutor()
    try:
        try:
            while futures or tasks:
                for k, v in dict(tasks).items():
                    if not v:
                        # submit tasks with no deps
                        futures[pool.submit(k, results[k])] = k
                        del tasks[k]
                done, pending = cf.wait(futures, return_when=cf.FIRST_COMPLETED)
                for f in done:
                    e = f.exception()
                    if e:
                        raise e
                    r = f.result()
                    yield r

                    # remove future
                    k = futures[f]
                    del futures[f]
                    # remove results
                    del results[k]

                    # remove task from deps
                    for k_, v in tasks.items():
                        if v is not None and k in v:
                            # add result to deps
                            results[k_].append(r)
                            v.remove(k)
        finally:
            pool.shutdown(wait=False)
    except KeyboardInterrupt as e:
        if pool:
            try:
                kill_pool(pool)
            finally:
                raise e


def kill_pool(pool: cf.Executor):
    # processes are terminated properly
    if isinstance(pool, cf.ThreadPoolExecutor):
        for t in pool._threads:  # type: ignore
            kill_thread(t)


def kill_thread(thread):
    assert thread.ident, 'thread has not been started'
    tid = ctypes.c_long(thread.ident)
    exc = ctypes.py_object(KeyboardInterrupt)
    ret = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, exc)
    if ret == 0:
        # raise ValueError('invalid thread id')
        pass
    elif ret != 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
        raise SystemError('PyThreadState_SetAsyncExc failed')


def humantime(t: float) -> str:
    """Formats time into a compact human readable format

    Parameters
    ----------
    t : float
        number of seconds
    """
    times = {}
    units = {'y': 31536000, 'w': 604800, 'd': 86400, 'h': 3600, 'm': 60, 's': 1}
    for i, (unit, seconds) in enumerate(units.items()):
        if t // seconds > 0:
            times[unit] = int(t//seconds)
            t -= t//seconds * seconds
    if not times:
        if int(t * 1000) > 0:
            times['ms'] = int(t * 1000)
        else:
            return '0s'
    return ''.join(f'{v}{u}' for u, v in times.items())


def humansize(s: int) -> str:
    """Formats byte size into a compact human readable format

    Parameters
    ----------
    s : int
        number of bytes
    """
    sizes = {}
    units = {'T': 0x10000000000, 'G': 0x40000000, 'M': 0x100000, 'k': 0x400, 'b': 1}
    for i, (unit, size) in enumerate(units.items()):
        if s // size > 0:
            sizes[unit] = s/size
            s -= s//size * size
    if 'b' not in sizes:
        return '0b'
    u, v = max(sizes.items(), key=lambda i: i[1]*units[i[0]])
    return f'{int(v)}{u}' if int(v) == v else f'{v:.2f}{u}'


def expand_path(path: str):
    return os.path.expandvars(os.path.expanduser(path))


def readlines(path, binary=False, skip_rows=0):
    path = expand_path(path)
    with open(path, mode='rb' if binary else 'r', encoding=None if binary else 'utf-8') as f:
        for i, line in enumerate(f):
            if i >= skip_rows:
                yield line.rstrip()


def writelines(lines, path, binary=False):
    path = expand_path(path)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, mode='wb' if binary else 'w', encoding=None if binary else 'utf-8') as f:
        for i, line in enumerate(lines):
            if (i > 0):
                f.write(b'\n' if binary else '\n')
            f.write(line)
