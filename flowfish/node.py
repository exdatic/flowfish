import asyncio
import copy
import functools
import inspect
import json
import os
from pathlib import Path
import re
import shutil
import time
from typing import Any, Callable, Dict, List, Optional, Set, Union, Tuple, TYPE_CHECKING
import warnings

import cloudpickle
import filelock

from flowfish.conf import (
    RewriteArgsConf, RewriteBaseConf, RewriteCallConf, RewriteFlowConf,
    RewriteHashConf, RewriteNodeConf
)
from flowfish.func import AsyncRegenerator, Func, Regenerator
from flowfish.error import FlowError, NodeNotFoundError, ScopeNotFoundError
from flowfish.graph import dot
from flowfish.utils import (
    copy_file, copy_props, hash32, humantime,
    pip_install, pool_run, resolve_packages, wrap_tqdm,
    readlines, writelines
)

if TYPE_CHECKING:
    from flowfish.flux import Flux
    from flowfish.flow import Flow
    from flowfish.scope import Scope
    from flowfish.link import Link  # noqa: F401


class Empty:
    pass


class Node:

    empty = Empty()

    _base_node: Optional['Node']
    _func: 'Func'
    _links: List['Link']
    _init_conf: Dict
    _base_conf: Dict
    _node_conf: Dict
    _args_conf: Dict
    _hash_conf: Dict

    def __init__(self, flux: 'Flux', flow: 'Flow', scope: 'Scope', name: str, conf: Dict):
        self._flux = flux
        self._flow = flow
        self._scope = scope
        self._name = name
        self._conf = conf

        # keep initial conf
        self._init_conf = copy.deepcopy(conf)

    @property
    def _logger(self):
        return self._flux.logger.opt(colors=True)

    @property
    def slug(self) -> str:
        return self._slug

    @property
    def _slug(self) -> str:
        return f'{self.base}.{self.hash}'

    @property
    def base(self) -> str:
        return self._conf['_base']

    @property
    def name(self) -> str:
        return self._name

    @property
    def hash(self) -> str:
        return self._hash

    @property
    def scope(self) -> str:
        return self._scope._name

    @property
    def _path(self) -> str:
        """base directory name of the node's scope"""
        # use value from scope unless set
        return self._conf.get('_path', self._scope._path)

    @property
    def _readonly(self) -> bool:
        # use value from scope unless set
        return self._conf.get('_readonly', self._scope._readonly)

    @property
    def _requires(self) -> Union[str, List[str]]:
        # use value from scope unless set
        return self._conf.get('_requires', self._scope._requires)

    @property
    def _base_dir(self):
        """base directory of the node's scope"""
        return self._data_dir / self._path

    @property
    def _data_dir(self):
        """data directory of the flow"""
        return self._flux.data_dir

    @property
    def path(self) -> Path:
        return self._work_dir

    @property
    def _work_dir(self) -> Path:
        return self._base_dir / self._slug

    @property
    def _data_file(self) -> Path:
        return self._base_dir / f'{self._slug}.data'

    @property
    def _conf_file(self) -> Path:
        return self._base_dir / f'{self._slug}.json'

    @property
    def _synced(self) -> bool:
        """check if node is synced to sync_dir"""
        return self._sync_file is not None and self._sync_file.is_file()

    @property
    def _sync_dir(self):
        return self._flux.sync_dir

    @property
    def _sync_file(self) -> Optional[Path]:
        if self._sync_dir:
            return self._sync_dir / self._path / '.sync' / f'{self._slug}.sync'
        else:
            return None

    @property
    def _locked(self) -> bool:
        if self._lock_file.is_file():
            try:
                lock = filelock.FileLock(str(self._lock_file))
                lock.acquire(0)
            except TimeoutError:
                return True
        return False

    @property
    def _lock_dir(self) -> Path:
        return self._base_dir / '.lock'

    @property
    def _lock_file(self) -> Path:
        return self._lock_dir / f'{self._slug}.lock'

    @property
    def doable(self) -> bool:
        """A node is doable if all dumpable dependenies are already dumped"""
        check_dumpable: Callable[[Node], bool] = lambda n: n._dumpable
        nodes, _ = self._tree(-1, until_done=check_dumpable)
        return all([n._dumped for n in nodes if n._dumpable])

    @property
    def done(self) -> bool:
        """A node is done if it is either cachable and cached or dumpable and dumped"""
        return self._done

    @property
    def _done(self) -> bool:
        return (self._cachable and self._cached) or (self._dumpable and self._dumped)

    @property
    def _cachable(self) -> bool:
        return self._conf.get('_cache', True)

    @property
    def _cached(self) -> bool:
        return self.data is not Node.empty

    @property
    def data(self):
        cache = self._flux.cache
        if self._slug in cache:
            return cache[self._slug]
        else:
            return Node.empty

    @data.setter
    def data(self, data):
        cache = self._flux.cache
        if data is Node.empty:
            del self.data
        else:
            cache[self._slug] = data

    @data.deleter
    def data(self):
        cache = self._flux.cache
        if self._slug in cache:
            del cache[self._slug]

    @property
    def _dumpable(self) -> bool:
        return self._conf.get('_dump', False)

    @property
    def _dumped(self) -> bool:
        """check if node is dumped to data_dir"""
        return self._data_file.is_file()

    @property
    def args(self) -> Dict:
        # create a copy
        return copy.deepcopy(self._args_conf)

    @property
    def conf(self) -> Dict:
        # create a copy
        return copy.deepcopy(self._flow_conf())

    def _node_crumb(self):
        if self._flow._file:
            return f'{self._flow._file.name}#{self._scope._name}.{self._name}'
        else:
            return f'{self._scope._name}.{self._name}'

    def _merge_node(self):
        base_node = self._resolve_base()
        if base_node:
            base_node._merge_node()

            self._conf['_func'] = base_node._conf['_func']
            self._conf['_base'] = base_node._conf['_base']

            # add missing properties from base node
            copy_props(base_node._conf, self._conf, overwrite=False)
        else:
            if '_func' not in self._conf:
                self._conf['_func'] = self._conf['_base']
                self._conf['_base'] = self._name
            elif '_base' not in self._conf:
                self._conf['_base'] = self._name

        # mark node as resolved and merged
        self._conf['_root'] = True

    def _resolve_base(self):
        if hasattr(self, '_base_node'):
            return self._base_node

        # node is marked as resolved and merged
        if self._conf.get('_root'):
            self._base_node = None
            return None

        base_name = self._conf['_base']

        try:
            # 1) search for base_node in another file, current scope, scope siblings and base_scopes
            try:
                base_node = self._scope._find_node(base_name)
            except (ScopeNotFoundError, NodeNotFoundError):
                base_node = None

            # 2) search for base_node in base_scopes only if self was returned
            if base_node == self:
                base_scope = self._scope._resolve_base()
                if base_scope:
                    try:
                        self._base_node = base_scope._find_node(base_name)
                    except (ScopeNotFoundError, NodeNotFoundError):
                        self._base_node = None
                else:
                    self._base_node = None
            elif base_node:
                self._base_node = base_node

            # 3) base_name must be a function
            else:
                self._base_node = None

            # 4) resolve all base_nodes recursively
            branch = []
            node = self
            while node:
                branch += [node]
                node = node._resolve_base()
                if node in branch:
                    loop = map(lambda n: f'[{n.scope}.{n.name}]' if node == n else f'{n.scope}.{n.name}', branch + [node])
                    raise RecursionError(f'Loop detected: {" @ ".join(loop)}')

            return self._base_node

        except FlowError as e:
            crumb = self._node_crumb()
            raise FlowError(f'{crumb} @ "{base_name}"') from e

    def _find_node(self, link) -> 'Node':
        node = self
        while node:
            try:
                return node._scope._find_node(link)
            except (ScopeNotFoundError, NodeNotFoundError):
                node = node._resolve_base()

        crumb = self._node_crumb()
        raise NodeNotFoundError(f'{crumb}: "{link}"')

    def _setup_node(self, branch=[]):
        if hasattr(self, '_node_conf'):
            return self

        # install requirements
        requirements = self._requires
        requirements = resolve_packages(*([requirements] if isinstance(requirements, str) else requirements))
        if requirements:
            self._logger.info(f'install requirements: {requirements}')
            pip_install(*requirements)

        # lookup function
        func = self._find_func(self._conf['_func'], self._conf.get('_code', None))
        if func.code:
            self._conf['_code'] = func.code
        self._func = func

        self._links = []
        self._base_conf = RewriteBaseConf(self, self._links).rewrite(self._conf)
        self._node_conf = RewriteNodeConf(self._func.defs).rewrite(self._base_conf)
        self._args_conf = RewriteArgsConf(self._func.defs).rewrite(self._node_conf)

        # link nodes
        self._flux.graph.add_node(self)
        for link in self._links:
            if link.source != self:
                self._flux.graph.add_link(link)

        # make this node look like a real function
        wrapped_func = self._func.func
        wrapped_docs = self._func.func.__doc__
        wrapped_sign = self._func.fake_sign(self.args)
        self.__wrapped__ = wrapped_func
        self.__doc__ = wrapped_docs
        self.__signature__ = wrapped_sign

        # ensure branch
        branch = branch + [self]
        for link in self._links:
            if link.source != self:
                node = link.source
                if node in branch:
                    loop = map(lambda n: f'[{n.scope}.{n.name}]' if node == n else f'{n.scope}.{n.name}',
                               branch + [node])
                    raise RecursionError(f'Loop detected: {" @ ".join(loop)}')
                node._setup_node(branch)

        # NOTE: A hash collision can occur if a node name is also used in another scope
        # with identical parameterization but different function, because the function
        # name is not included in the hashing.

        self._hash_conf = {self.base: RewriteHashConf().rewrite(self._node_conf)}

        # create new hash or reuse existing hash
        if '_hash' not in self._node_conf:
            dump = json.dumps(self._hash_conf, sort_keys=True)
            self._hash = hash32(dump)
            self._base_conf['_hash'] = self._hash
            self._node_conf['_hash'] = self._hash
        else:
            hash_ = self._node_conf['_hash']
            if not isinstance(hash_, str) or not re.match(r'[a-z0-9]{1,8}', hash_):
                raise ValueError(f'"_hash" must be a 32-bit hex string and not: {hash_}')
            self._hash = hash_

        return self

    def _find_func(self, name: str, code: Optional[str] = None) -> 'Func':
        funcs = self._flux.funcs
        if name not in funcs:
            func = Func.find(name, code)
            funcs[name] = func
        else:
            func = funcs[name]
        return func

    def __call__(self, *args, **kwargs):
        if args or kwargs:
            return self._with_args(*args, **kwargs)()

        # run missing steps on agent and wait for completion
        agent = self._conf.get('_agent', None)
        if agent and not self._done and not self._readonly:
            self._wait_for_job(agent)

        return self._call()

    def _with_args(self, *args, **kwargs) -> 'Node':
        return self + self._func.call_args(*args, **kwargs)

    def _wait_for_job(self, agent: str):
        # run missing steps on agent and wait for completion
        assert self._sync_dir, "sync_dir is not set"

        def _copy_done(node: 'Node'):
            for n in node._deps():
                if (n._dumpable and n._dumped) and not n._synced:
                    n.push(copy_all=True)
                if not (n._done or n._synced):
                    _copy_done(n)

        # copy deps that are already done and not yet synced
        # TODO copy work_dir of self and deps even if not yet done
        _copy_done(self)

        def _find_todo(node: 'Node', todos: Set['Node']):
            if not node._done and not node._synced and node._dumpable:
                todos.add(node)
            elif not (node._done or node._synced):
                for n in node._deps():
                    _find_todo(n, todos)

        # find todos, e.g. a -> *b:dumpable -> c:dumpable -> d:dumped|synced
        todos: Set['Node'] = set()
        _find_todo(self, todos)

        # create job if necessary
        if todos:
            self._create_job(agent, self._sync_dir)

        # wait for todos to pull from agent
        while todos:
            for todo in set(todos):
                if todo._synced:
                    todo.pull()
                    todos.remove(todo)
                else:
                    time.sleep(1)

    def _create_job(self, agent: str, data_dir: Path):
        job_file = data_dir / self._path / '.jobs' / f'{self._slug}.{agent}.json'
        self._logger.info(
            f'<c>{self.scope}</c>: send <c>{self.name}</c> to "{agent}" ({job_file})')
        os.makedirs(job_file.parent, exist_ok=True)
        # overwrite job file
        with open(job_file, 'w') as f:
            json.dump(self._flow_conf(), f, sort_keys=True, indent=4)

    def _call(self):
        nodes, links = self._tree(-1, until_done=lambda n: n._done)
        byval_sources = set(link.source for link in links if link.kind == '@')
        byval_sources.add(self)  # don't forget to add self
        byref_sources = set(link.source for link in links if link.kind == '&')

        funcs = dict((n, functools.partial(self._call_node, n, byval_sources, byref_sources)) for n in nodes)
        tasks = dict((funcs[n], [funcs[d] for d in n._deps()] if not n._done else None)
                     for n in nodes)

        for n, (v, r) in pool_run(tasks):  # type: ignore
            if n == self:
                return v

    def _deps(self) -> Set['Node']:
        return set(link.source for link in self._links if link.source != self)

    def _call_node(
        self,
        node: 'Node',
        byval_sources: Set['Node'],
        byref_sources: Set['Node'],
        vals: List[Tuple['Node', Any]]
    ):
        if (node._dumpable and not node._dumped) and node._readonly:
            self._logger.warning(
                f'{node.scope}: {node.name} is read-only and therefore not callable')
            raise FlowError(f'node read-only: {repr(node)}')

        try:
            needs_val = node in byval_sources
            needs_ref = node in byref_sources
            with node._lock():
                r, v = None, None
                if needs_val:
                    v = node._call_now(dict(vals))
                if needs_ref:
                    r = node._call_later(dict(vals))
                return (node, (v, r))
        except Exception as e:
            raise FlowError(f'call failed: {repr(node)}') from e

    def _lock(self):
        if self._dumpable or self._work_dir.is_dir():
            os.makedirs(self._lock_dir, exist_ok=True)
            lock = filelock.FileLock(str(self._lock_file))
            try:
                with lock.acquire(0):
                    pass
            except TimeoutError:
                self._logger.info(
                    f'<c>{self.scope}</c>: <c>{self.name}</c> is waiting for lock')
            return lock
        else:
            return self._flux.locks[self._slug]

    def _call_later(self, vals: Dict['Node', Any]):
        def _call(*args, **kwargs):
            # only log first call of a reference
            stats['call_count'] += 1
            if stats['call_count'] == 1:
                self._logger.info(
                    f'<c>{self.scope}</c>: call <c>{self.name}</c> {{}}', self.args)
            call_time = time.time()
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore")
                v = self._call_func(vals, *args, **kwargs)
            call_time = time.time() - call_time
            if stats['call_count'] == 1:
                self._logger.info(
                    f'<c>{self.scope}</c>: call <c>{self.name}</c> took {humantime(call_time)}+',
                    duration=call_time)
            return v

        stats = {'call_count': 0}
        return _call

    def _call_now(self, vals: Dict['Node', Any]):
        v = self.data
        if self._cachable and self._cached:
            # log loading of cached steps as debug
            self._logger.debug(
                f'<c>{self.scope}</c>: load <c>{self.name}</c> from cache')
        elif self._dumpable and self._dumped:
            self._logger.info(
                f'<c>{self.scope}</c>: load <c>{self.name}</c> from {self._work_dir}')
            load_time = time.time()
            v = self._load_data()
            load_time = time.time() - load_time
            self._logger.info(
                f'<c>{self.scope}</c>: load <c>{self.name}</c> took {humantime(load_time)}',
                duration=load_time)
            # do not cache generators
            if self._cachable and not isinstance(v, (Regenerator, AsyncRegenerator)):
                self.data = v
        else:
            self._logger.info(
                f'<c>{self.scope}</c>: call <c>{self.name}</c> {{}}', self.args)

            call_time = time.time()
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore")
                v = self._call_func(dict(vals))
            call_time = time.time() - call_time

            if self._dumpable:
                self._logger.info(
                    f'<c>{self.scope}</c>: call <c>{self.name}</c> took {humantime(call_time)} ({self._work_dir})',
                    duration=call_time)
                self._dump_conf()
                self._dump_data(v)
            elif self._work_dir.is_dir():
                self._logger.info(
                    f'<c>{self.scope}</c>: call <c>{self.name}</c> took {humantime(call_time)} ({self._work_dir})',
                    duration=call_time)
                self._dump_conf()
            else:
                self._logger.info(
                    f'<c>{self.scope}</c>: call <c>{self.name}</c> took {humantime(call_time)}',
                    duration=call_time)
            # do not cache generators
            if self._cachable and not isinstance(v, (Regenerator, AsyncRegenerator)):
                self.data = v

        return v

    def _call_func(self, vals: Dict['Node', Any], *args, **kwargs):
        call_conf = self._call_conf(vals)

        # inject flow logger
        if '_logger' in call_conf and call_conf['_logger'] is None:
            call_conf['_logger'] = self._logger

        v = self._func.call(*args, **{**kwargs, **call_conf})

        if inspect.iscoroutine(v):
            v = self._call_coro(v)

        # wrap generators with tqdm progress bar
        if self._conf.get('_tqdm', False) and isinstance(v, (Regenerator, AsyncRegenerator)):
            return wrap_tqdm(self.name, v)

        return v

    def _call_conf(self, vals: Dict['Node', Any]) -> Dict:
        node_conf = self._node_conf
        call_conf = RewriteCallConf(self._flux.funcs, self._func.pars, vals).rewrite(node_conf)
        return call_conf

    def _call_coro(self, coro):
        loop = self._flux.loop or self._flux.loop_thread.loop()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        result = future.result()
        return result

    def _load_conf(self):
        if self._conf_file.is_file():
            with open(self._conf_file, 'r') as conf_file:
                json.load(conf_file)

    def _load_data(self):
        if self._data_file.is_file():
            with open(self._data_file, 'rb') as f:
                return cloudpickle.load(f)
        else:
            return Node.empty

    def save(self, filename: Optional[Union[str, Path]] = None):
        if filename:
            with open(filename, 'w') as f:
                json.dump(self._flow_conf(), f, sort_keys=True, indent=4)
            self._logger.info(
                f'<c>{self.scope}</c>: save <c>{self.name}</c> to {filename}')
        else:
            self._dump_conf()
            self._logger.info(
                f'<c>{self.scope}</c>: save <c>{self.name}</c>, load with "{self._path}#{self._slug}"')

    def _dump_conf(self):
        os.makedirs(self._base_dir, exist_ok=True)
        with open(self._conf_file, 'w') as f:
            json.dump(self._flow_conf(), f, sort_keys=True, indent=4)

    def _flow_conf(self) -> Dict:
        base_conf = self._base_conf
        scope_conf = {self.name: RewriteFlowConf().rewrite(base_conf)}

        # dump scope _path
        if self._scope._path != self._scope._name:
            scope_conf['_path'] = self._scope._path

        # merge flow confs
        flow_conf = {self.scope: scope_conf}
        for n in self._deps():
            # only add confs from same flow
            # TODO support inter-flow conf
            if n != self and n._flow is self._flow:
                for k, v in n._flow_conf().items():
                    if k not in flow_conf:
                        flow_conf[k] = dict()
                    flow_conf[k].update(v)
        return flow_conf

    def _dump_data(self, data):
        os.makedirs(self._base_dir, exist_ok=True)
        temp_file = self._data_file.with_name(self._data_file.name + '.tmp')
        with open(temp_file, 'wb') as f:
            # NOTE: cloudpickle and pickle are compatible,
            # so it can be used as a drop-in replacement
            cloudpickle.dump(data, f, protocol=4)
        temp_file.rename(self._data_file)

    def _tree(
        self,
        direction: int = 1,
        until_done: Union[bool, Callable[['Node'], bool]] = False,
        omit_internal: bool = False
    ) -> Tuple[List['Node'], List['Link']]:
        return self._flux.graph.tree(self, direction=direction, until_done=until_done, omit_internal=omit_internal)

    def dot(self, direction=-1, until_done=False, omit_internal=False):
        """the DOT graph of the node"""
        return dot(self._flux.graph, self, direction, until_done, omit_internal)

    def purge(self):
        """
        .. deprecated:: 3.1
            Use :func:`wipe` instead.
        """
        self.wipe()

    def wipe(self):
        """clear data of node and all descendant nodes"""

        nodes, links = self._tree(1)
        for n in nodes:
            if n._done:
                self._logger.info(
                    f'<c>{n.scope}</c>: wipe <c>{n.name}</c> from {n._work_dir}')
            n.clear()

    def clear(self):
        """clear node data"""

        # delete cached value
        del self.data

        # delete work dir
        if self._work_dir.is_dir():
            shutil.rmtree(self._work_dir, ignore_errors=True)

        # delete data file
        if self._data_file.is_file():
            os.remove(self._data_file)

        # delete conf file
        if self._conf_file.is_file():
            os.remove(self._conf_file)

    def push(self, copy_all: bool = False):
        """push dumped steps of node dependency tree to sync_dir"""
        assert self._sync_dir is not None, "sync dir not set"

        source_dir = self._data_dir
        target_dir = self._sync_dir

        nodes, _ = self._tree(-1, until_done=lambda n: n._dumped or n._synced)

        for node in nodes:
            assert node._sync_file is not None, "sync dir not set"

            if copy_all:
                node_dumped = node._dumpable and node._dumped or node._work_dir.is_dir() and node._conf_file.is_file()
            else:
                node_dumped = node._dumpable and node._dumped

            if node_dumped and not node._synced:
                """copy all node files and create .sync file"""
                self._logger.info(
                    f'<c>{node.scope}</c>: push <c>{node.name}</c> to {target_dir / node._path / node._slug}')

                files = []  # keep order of insertion

                # scan work_dir
                for f in node._work_dir.glob('**/*'):
                    if f.is_file():
                        files.append(f.relative_to(source_dir))

                # add conf file
                if node._conf_file.is_file():
                    files.append(node._conf_file.relative_to(source_dir))

                # add data file last
                if node._data_file.is_file():
                    files.append(node._data_file.relative_to(source_dir))

                # copy files
                for f in files:
                    src = source_dir / f
                    dst = target_dir / f
                    copy_file(src, dst)

                # track files
                if not node._sync_file.is_file():
                    os.makedirs(node._sync_file.parent, exist_ok=True)
                    writelines((str(f) for f in files), node._sync_file)

    def pull(self):
        """pull dumped steps of node dependency tree from sync_dir"""
        assert self._sync_dir is not None, "sync dir not set"

        source_dir = self._sync_dir
        target_dir = self._data_dir

        nodes, _ = self._tree(-1, until_done=lambda n: (n._dumpable and n._dumped) or n._synced)

        for node in nodes:
            if not (node._dumpable and node._dumped) and node._synced:
                """sync all node files defined in .sync file"""
                self._logger.info(
                    f'<c>{node.scope}</c>: pull <c>{node.name}</c> from {source_dir / node._path / node._slug}')

                if node._synced:
                    files = set(readlines(node._sync_file))
                    while files:
                        for f in set(files):
                            src = source_dir / f
                            dst = target_dir / f
                            if src.is_file():
                                files.remove(f)
                                copy_file(src, dst)
                        if files:
                            time.sleep(1)

    def _copy_data(self, target: 'Node'):
        """copy node data to otehr node"""

        self._logger.info(f'<c>{self.scope}</c>: copy <c>{self.name}</c> to {target}')

        # scan work_dir
        for f in self._work_dir.glob('**/*'):
            if f.is_file():
                copy_file(f, target._work_dir / f.relative_to(self._work_dir))

        # add data file last
        if self._data_file.is_file():
            copy_file(self._data_file, target._data_file)

        # finally dump conf
        target._dump_conf()

    def __add__(self, props: Dict) -> 'Node':
        new_flow = self._flow + dict((f'{self.scope}.{self.name}.{k}', v)
                                     for k, v in props.items())
        return getattr(getattr(new_flow, self.scope), self.name)

    def __neg__(self):
        return dot(self._flux.graph, self, -1, omit_internal=False)

    def __pos__(self):
        return dot(self._flux.graph, self, +0, omit_internal=False)

    def __repr__(self):
        node = self
        base = node._conf.get('_base', None)
        func = node._conf.get('_func', None)
        value = f'{node._scope._name}.{node._name}'
        if base and base != node._name:
            value += f'@{base}'
        if func:
            value += f'[{func}]'
        return value
