from asyncio.events import AbstractEventLoop
import copy
import json
from pathlib import Path
import re
from typing import Dict, Optional, Tuple, Union, TYPE_CHECKING

from flowfish.error import FlowError
from flowfish.flow import Flow
from flowfish.graph import Graph
from flowfish.locks import KeyedLocks
from flowfish.logger import logger
from flowfish.utils import LoopThread, copy_props

if TYPE_CHECKING:
    from flowfish.func import Func


class Flux:

    graph: 'Graph'
    flows: Dict[Path, 'Flow']
    funcs: Dict[str, 'Func']
    locks: KeyedLocks
    loop: Optional[AbstractEventLoop]

    def __init__(self,
                 data_dir: Path,
                 sync_dir: Optional[Path],
                 funcs: Dict[str, 'Func'],
                 cache: Dict,
                 locks: KeyedLocks,
                 share: bool,
                 loop: Optional[AbstractEventLoop],
                 loop_thread: Optional[LoopThread] = None,
                 logger=logger):

        self.data_dir = data_dir
        self.sync_dir = sync_dir
        self.graph = Graph()
        self.flows = dict()
        self.locks = locks
        self.funcs = funcs
        self.cache = cache
        self.share = share
        self.loop = loop
        self.loop_thread = loop_thread or LoopThread()

        # bind logger enable one logger per flow
        self.logger = logger.bind(flux=id(self))

    def __iter__(self):
        return iter(self.flows.values())

    def copy(self):
        return Flux(
            self.data_dir,
            self.sync_dir,
            self.funcs if self.share else dict(self.funcs),
            self.cache if self.share else dict(self.cache),
            self.locks if self.share else KeyedLocks(),
            self.share,
            self.loop,
            self.loop_thread,
            self.logger,
        )

    def load_conf(self, file: Union[Path, str], props: Optional[Dict] = None, relative_to: Optional[Path] = None
                  ) -> Tuple[Path, Dict]:
        # 1.1) try to resolve node relative to data_dir (e.g. foo#bar.1234ab )
        if isinstance(file, str) and re.match(r'^\w+#\w+\.[a-z0-9]{1,8}$', file, re.ASCII):
            path, slug = file.split('#')
            conf_file = self.data_dir / path / f'{slug}.json'

        # 1.2) try to resolve flow relative to data_dir (e.g. flow.1234ab.json)
        elif isinstance(file, str) and re.match(r'^\w+\.[a-z0-9]{1,8}\.json$', file, re.ASCII):
            conf_file = self.data_dir / file

        # 1.3) try to resolve relative to current working directory
        else:
            conf_file = Path.cwd() / file

        # 2) try to resolve relative to data_dir
        if not conf_file.is_file():
            conf_file = self.data_dir / file

        # 3) try to resolve relative to calling flow.json
        if not conf_file.is_file() and relative_to:
            conf_file = relative_to.parent / file

        # 4) just take it as it is
        if not conf_file.is_file():
            conf_file = Path(file).absolute()

        # only load file based flows with no props from cache
        if conf_file in self.flows and not props:
            return conf_file, self.flows[conf_file]._conf
        else:
            if not conf_file.is_file():
                raise FlowError(f'Flow not found: {file}')

            with open(conf_file, 'r') as f:
                if conf_file.suffix.lower() in ('.yml', '.yaml'):
                    try:
                        import yaml
                    except ImportError:
                        raise ImportError(
                            "PyYAML not installed: "
                            "pip install pyyaml")
                    return conf_file, yaml.safe_load(f)
                else:
                    return conf_file, json.load(f)

    def load_flow(self, file: Union[Path, str], props: Optional[Dict] = None, relative_to: Optional[Path] = None):
        conf_file, conf = self.load_conf(file, props, relative_to)

        # only load file based flows with no props from cache
        if conf_file in self.flows and not props:
            return self.flows[conf_file]
        else:
            flow = self.make_flow(conf, props, conf_file)

            # track flow by name, save flow.json to data_dir
            if not flow._readonly:
                flow._save()

            return flow

    def make_flow(self, conf: Dict, props: Optional[Dict] = None, file: Optional[Path] = None):
        # Attention: flow must be known to others before setup!
        flow = self._add_flow(conf, props, file)
        flow._setup_flow()
        return flow

    def _add_flow(self, conf: Dict, props: Optional[Dict] = None, file: Optional[Path] = None):
        # clone original conf before modification
        flow_conf = copy.deepcopy(conf)

        # merge props
        flow_props = dict()
        copy_props(flow_conf.get('_props', None), flow_props)
        copy_props(props, flow_props)

        # copy hidden props to conf
        hidden_props = dict((k, v) for k, v in flow_props.items()
                            if k.startswith('_') and k != '_props')
        copy_props(hidden_props, flow_conf)

        flow = Flow(self, flow_conf, flow_props, file)

        # only store file based flows with no props in cache
        if file is not None and not props:
            self.flows[file] = flow

        return flow
