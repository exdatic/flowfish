import copy
import json
from pathlib import Path
import re
from typing import Dict, Iterator, List, Optional, Union, TYPE_CHECKING

from flowfish.error import FlowError
from flowfish.graph import dot
from flowfish.scope import Scope
from flowfish.utils import copy_props, hash32


if TYPE_CHECKING:
    from flowfish.flux import Flux


class Flow:

    _scopes: Dict[str, 'Scope']

    def __init__(self, flux: 'Flux', conf: Dict, props: Dict, file: Optional[Path] = None):
        self._scopes = dict()
        self._flux = flux
        self._props = props
        self._file = file

        # keep initial conf
        self._conf = copy.deepcopy(conf)

        # add scopes
        for scope_name, scope_conf in conf.items():
            if scope_name.startswith('#') or scope_name.startswith('_'):
                continue
            self._add_scope(scope_name, scope_conf)

    @property
    def _logger(self):
        return self._flux.logger.opt(colors=True)

    @property
    def _name(self) -> str:
        return self._conf.get('_name', None)

    @property
    def _hash(self):
        return hash32('|'.join(sorted(node._slug for scope in self for node in scope)))

    @property
    def _readonly(self) -> bool:
        return self._conf.get('_readonly', False)

    @property
    def _requires(self) -> Union[str, List[str]]:
        return self._conf.get('_requires', [])

    @property
    def _data_dir(self):
        return self._flux.data_dir

    def _scope_crumb(self, scope: str):
        if self._file:
            return f'{self._file.name}#{scope}'
        else:
            return f'{scope}'

    def _add_scope(self, scope_name, scope_conf):
        # yaml support: replace null by empty dict
        if scope_conf is None:
            scope_conf = dict()
        # ensure scope conf is a dict
        if not isinstance(scope_conf, dict):
            crumb = self._scope_crumb(scope_name)
            raise FlowError(f'{crumb}: conf must be dict and not {type(scope_conf)}')

        # ensure base name is set
        if '@' in scope_name:
            scope_name, base_name = scope_name.split('@', 1)
            scope_conf['_base'] = base_name
        elif '_base' not in scope_conf:
            scope_conf['_base'] = scope_name

        if not re.match(r'^\w+$', scope_name, re.ASCII):
            crumb = self._scope_crumb(scope_name)
            raise FlowError(f'{crumb}: invalid scope name')

        # merge props
        scope_props = dict()
        copy_props(self._conf.get('_props', None), scope_props, [scope_name])
        copy_props(scope_conf.get('_props', None), scope_props)
        copy_props(self._props, scope_props, [scope_name])

        # copy hidden props to conf
        hidden_props = dict((k, v) for k, v in scope_props.items()
                            if k.startswith('_') and k != '_props')
        copy_props(hidden_props, scope_conf)

        scope = Scope(self._flux, self, scope_name, scope_conf, scope_props)
        self._scopes[scope_name] = scope
        setattr(self, scope_name, scope)
        return scope

    def _setup_flow(self):
        # merge scopes
        for scope in self._scopes.values():
            scope._merge_scope()

        # merge nodes
        for scope in self._scopes.values():
            scope._merge_nodes()

        # setup nodes and links
        for scope in self._scopes.values():
            scope._setup_nodes()

    def _save(self):
        if self._name:
            if not re.match(r'^\w+$', self._name, re.ASCII):
                raise ValueError(f'"_name" contains invalid chars (must be alphanumeric): {self._name}')
            name = self._name
        elif self._file:
            name = self._file.with_suffix('').name
        else:
            name = 'flow'

        hash_ = self._hash
        if name.endswith('.' + hash_):
            conf_file = self._data_dir / f'{name}.json'
        else:
            conf_file = self._data_dir / f'{name}.{hash_}.json'

        if conf_file.is_file():
            # conf_file.touch(exist_ok=True)
            self._logger.debug(f'flow found, load with "{conf_file.name}"')
        else:
            conf_file.parent.mkdir(exist_ok=True)
            with open(conf_file, 'w') as f:
                json.dump(self._flow_conf(), f, sort_keys=True, indent=4)
            self._logger.info(f'flow saved, load with "{conf_file.name}"')

    def _flow_conf(self):
        flow_conf = dict()
        for scope in self:
            for node in scope:
                for scope_name, scope_conf in node._flow_conf().items():
                    if scope_name not in flow_conf:
                        flow_conf[scope_name] = dict()
                    for node_name, node_conf in scope_conf.items():
                        if node_name not in flow_conf[scope_name]:
                            flow_conf[scope_name][node_name] = node_conf
        return flow_conf

    def __iter__(self) -> Iterator['Scope']:
        return iter(self._scopes.values())

    def __call__(self):
        # auto run steps with _run=True
        for scope in self._scopes.values():
            scope()

    def __getitem__(self, key):
        return self._scopes[key]

    def __contains__(self, key):
        return key in self._scopes

    def __add__(self, props: Dict) -> 'Flow':
        new_props = dict(self._props) if self._props else dict()
        new_props.update(props)

        new_flux = self._flux.copy()
        if self._file:
            return new_flux.load_flow(self._file, props=new_props)
        else:
            return new_flux.make_flow(self._conf, props=new_props)

    def __neg__(self):
        return dot(self._flux.graph, None, -1, omit_internal=False, scopes=set(self._scopes.values()))

    def __pos__(self):
        return dot(self._flux.graph, None, +0, omit_internal=False, scopes=set(self._scopes.values()))
