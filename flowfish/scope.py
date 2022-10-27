import copy
from pathlib import Path
import re
from typing import Dict, Iterator, List, Union, TYPE_CHECKING

from flowfish.error import FlowError, NodeNotFoundError, ScopeNotFoundError
from flowfish.graph import dot
from flowfish.node import Node
from flowfish.utils import copy_props

if TYPE_CHECKING:
    from flowfish.flux import Flux
    from flowfish.flow import Flow


class Scope:

    _nodes: Dict[str, 'Node']

    def __init__(self, flux: 'Flux', flow: 'Flow', name: str, conf: Dict, props: Dict):
        self._nodes = dict()
        self._flux = flux
        self._flow = flow
        self._name = name
        self._conf = conf
        self._props = props

        # keep initial conf
        self._init_conf = copy.deepcopy(conf)

        # add nodes
        for node_name, node_conf in conf.items():
            if node_name.startswith('#') or node_name.startswith('_'):
                continue
            self._add_node(node_name, node_conf)

        # add missing nodes from props, e.g. _props.foo=@bar -> foo@bar
        for k, v in self._props.items():
            # skip comments
            if k.startswith('#') or k.startswith('_'):
                continue
            if '.' not in k and type(v) is str and v.startswith('@') and k not in self._nodes:
                self._add_node(k + v, dict())

    @property
    def _logger(self):
        return self._flux.logger.opt(colors=True)

    @property
    def _base(self) -> str:
        return self._conf['_base']

    @property
    def _path(self) -> str:
        """base directory name of the scope"""
        return self._conf.get('_path', self._flow._conf.get('_path', self._name))

    @property
    def _readonly(self) -> bool:
        return self._conf.get('_readonly', self._flow._readonly)

    @property
    def _requires(self) -> Union[str, List[str]]:
        return self._conf.get('_requires', self._flow._requires)

    @property
    def _base_dir(self) -> Path:
        """base directory of the scope"""
        return self._data_dir / self._path

    @property
    def _data_dir(self):
        """data directory of the flow"""
        return self._flux.data_dir

    def _node_crumb(self, node: Union[str, 'Node']):
        node_name = node if isinstance(node, str) else node._name
        if self._flow._file:
            return f'{self._flow._file.name}#{self._name}.{node_name}'
        else:
            return f'{self._name}.{node_name}'

    def _scope_crumb(self):
        if self._flow._file:
            return f'{self._flow._file.name}#{self._name}'
        else:
            return f'{self._name}'

    def _add_node(self, node_name, node_conf):
        # yaml support: replace null by empty dict
        if node_conf is None:
            node_conf = dict()
        # ensure node conf is a dict
        if not isinstance(node_conf, dict):
            crumb = self._node_crumb(node_name)
            raise FlowError(f'{crumb}: conf must be dict and not {type(node_conf)}')

        # ensure base name is set
        if '@' in node_name:
            node_name, base_name = node_name.split('@', 1)
            node_conf['_base'] = base_name
        elif '_base' not in node_conf:
            node_conf['_base'] = node_name

        if not re.match(r'^\w+$', node_name, re.ASCII):
            crumb = self._node_crumb(node_name)
            raise FlowError(f'{crumb}: invalid node name')

        if node_name in self._props:
            prop_value = self._props.get(node_name)

            # override node conf with dict: foo={}
            if type(prop_value) is dict:
                copy_props(prop_value, node_conf, [])

            # override node conf with pydantic model
            if hasattr(prop_value, 'dict'):
                copy_props(prop_value.dict(), node_conf, [])  # type: ignore

            # override base with props: foo=@bar -> foo@bar
            if type(prop_value) is str:
                base_name = prop_value
                if not base_name.startswith('@'):
                    crumb = self._node_crumb(node_name)
                    raise FlowError(f'{crumb} @ "{base_name}" should be "@{base_name}"')
                node_conf['_base'] = base_name[1:]
                node_conf.pop('_func', None)

        # model@torchvision.models$model_name {model_name: resnet50} ->
        #    model@torchvision.models.resnet50: {}
        if '$' in node_conf['_base']:
            module, callee = node_conf['_base'].split('$', 1)
            node_conf['_func'] = module + '.' + node_conf.pop(callee)
            node_conf['_base'] = node_name

        # override node conf with flat props
        copy_props(self._props, node_conf, [node_name])

        node = Node(self._flux, self._flow, self, node_name, node_conf)
        self._nodes[node_name] = node
        setattr(self, node_name, node)
        return node

    def _resolve_base(self):
        if hasattr(self, '_base_scope'):
            return self._base_scope

        # lookup base name
        base_name = self._conf['_base']

        try:
            # 1) search for base_scope
            base_scope = self._find_scope(base_name)
            if base_scope != self:
                self._base_scope = base_scope
            else:
                self._base_scope = None

            # 2) resolve all base_scopes recursively
            branch = []
            scope = self
            while scope:
                branch += [scope]
                scope = scope._resolve_base()
                if scope in branch:
                    loop = map(lambda s: f'[{s.name}]' if scope == s else f'{s.name}', branch + [scope])
                    raise RecursionError(f'loop detected: {" @ ".join(loop)}')

            return self._base_scope

        except FlowError as e:
            crumb = self._scope_crumb()
            raise FlowError(f'{crumb} @ "{base_name}"') from e

    def _merge_scope(self):
        base_scope = self._resolve_base()
        if base_scope:
            base_scope._merge_scope()

            # copy base name from base scope
            self._conf['_base'] = base_scope._conf['_base']

            # add missing hidden properties from base scope
            hidden_props = dict((k, v) for k, v in base_scope._conf.items()
                                if k.startswith('_'))
            copy_props(hidden_props, self._conf, overwrite=False)

            # add missing nodes from base scope
            for node_name, base_node in base_scope._nodes.items():
                if node_name not in self._nodes:
                    node_conf = dict()
                    copy_props(base_node._init_conf, node_conf)
                    self._add_node(node_name, node_conf)
        else:
            # set base name to scope name
            if '_base' not in self._conf:
                self._conf['_base'] = self._name

    def _merge_nodes(self):
        for node in self._nodes.values():
            node._merge_node()

    def _setup_nodes(self):
        for node in self._nodes.values():
            node._setup_node()

    def _find_scope(self, link) -> 'Scope':
        # search for scope in file
        if re.match(r'^[^#]+#\w+$', link, re.ASCII):
            file, name = link.split('#')

            flow = self._flux.load_flow(file, relative_to=self._flow._file)
            if name in flow._scopes:
                return flow._scopes[name]

        # search for scope in flow
        elif re.match(r'^\w+$', link, re.ASCII):
            if link in self._flow._scopes:
                return self._flow._scopes[link]

        crumb = self._scope_crumb()
        raise ScopeNotFoundError(f'{crumb}: "{link}"')

    def _find_node(self, link) -> 'Node':
        # search for @foo#bar.1234ab
        if re.match(r'^\w+#\w+\.[a-z0-9]{1,8}$', link, re.ASCII):
            path, slug = link.split('#')
            conf_file = self._data_dir / path / f'{slug}.json'
            if conf_file.is_file():
                flow = self._flux.load_flow(conf_file)
                for scope in flow:
                    for node in scope:
                        if node._slug == slug and node._path == path:
                            return node
            else:
                raise FlowError(f'{conf_file} not found')

        # search for node in file
        if re.match(r'^[^#]+#\w+\.\w+$', link, re.ASCII):
            file, step = link.split('#')
            scope_name, node_name = step.split('.')

            flow = self._flux.load_flow(file, relative_to=self._flow._file)
            if scope_name in flow._scopes:
                scope = flow._scopes[scope_name]
                return scope._find_node(node_name)

        # search for node in flow and base flows
        if re.match(r'^\w+\.\w+$', link, re.ASCII):
            scope_name, node_name = link.split('.')
            flow, scope = self._flow, self
            while scope:
                if flow and scope_name in flow._scopes:
                    scope = flow._scopes[scope_name]
                if node_name in scope._nodes:
                    node = scope._nodes[node_name]
                    return node
                scope = scope._resolve_base()
                flow = scope._flow if scope else None

        # search for node in scope and base scopes
        if re.match(r'^\w+$', link, re.ASCII):
            scope = self
            node_name = link
            while scope:
                if node_name in scope._nodes:
                    node = scope._nodes[node_name]
                    return node
                scope = scope._resolve_base()

        crumb = self._scope_crumb()
        raise NodeNotFoundError(f'{crumb}: "{link}"')

    def __getitem__(self, key):
        return self._nodes[key]

    def __contains__(self, key):
        return key in self._nodes

    def __setattr__(self, name, value):
        if hasattr(self, name):
            node = getattr(self, name)
            if isinstance(node, Node) and isinstance(value, Node):
                value._copy_data(node)
                return
        super().__setattr__(name, value)

    def __delattr__(self, name):
        if hasattr(self, name):
            node = getattr(self, name)
            if isinstance(node, Node):
                node.wipe()
                return
        super().__delattr__(name)

    def __add__(self, props: Dict) -> 'Scope':
        new_flow = self._flow + dict((f'{self._name}.{k}', v) for k, v in props.items())
        return getattr(new_flow, self._name)

    def __iter__(self) -> Iterator['Node']:
        return iter(self._nodes.values())

    def __call__(self):
        # auto run steps with _run=True
        for node in self._nodes.values():
            if node._conf.get('_run', False):
                try:
                    node()
                except Exception as e:
                    self._logger.info(
                        f'<c>{node.scope}</c>: <c>{node.name}</c> failed with {e.__cause__!r}')

    def __neg__(self):
        return dot(self._flux.graph, None, +0, omit_internal=False, scopes=set([self]))

    def __pos__(self):
        return dot(self._flux.graph, None, +0, omit_internal=False, scopes=set([self]))

    def __repr__(self):
        scope = self
        value = scope._name
        base = scope._conf['_base']
        if base and base != scope._name:
            value += f'@{base}'
        return value
