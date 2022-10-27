import inspect
import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple, Union, TYPE_CHECKING

import cloudpickle

from flowfish.error import FlowError, NodeNotFoundError
from flowfish.func import Func
from flowfish.link import Link
from flowfish.utils import fake_hash, hash32, sorted_dict

if TYPE_CHECKING:
    from flowfish.node import Node


class StopRewrite(Exception):
    pass


class Rewrite:

    def __init__(self, max_depth=-1):
        self._max_depth = max_depth

    def discard_item(self, k, v: Any, depth: int, parent=None):
        return False

    def rewrite(self, v: Any) -> Any:
        return self._rewrite(None, v, depth=0)

    def _rewrite(self, k: Optional[str], v: Any, depth: int):
        if self._max_depth != -1 and depth > self._max_depth:
            return v

        try:
            if isinstance(v, str):
                return self.rewrite_str(k, v, depth)
            elif v is None:
                return self.rewrite_none(k, v, depth)
            elif v is True:
                return self.rewrite_bool(k, v, depth)
            elif v is False:
                return self.rewrite_bool(k, v, depth)
            elif isinstance(v, int):
                return self.rewrite_int(k, v, depth)
            elif isinstance(v, float):
                return self.rewrite_float(k, v, depth)
            elif isinstance(v, (list, tuple)):
                return self.rewrite_list(k, self._rewrite_list(k, v, depth), depth)
            elif isinstance(v, dict):
                return self.rewrite_dict(k, self._rewrite_dict(k, v, depth), depth)
            else:
                return self.rewrite_object(k, v, depth)
        except StopRewrite:
            pass

    def rewrite_none(self, k, v, depth):
        return v

    def rewrite_bool(self, k, v: bool, depth: int):
        return v

    def rewrite_int(self, k, v: int, depth: int):
        return v

    def rewrite_float(self, k, v: float, depth: int):
        return v

    def rewrite_str(self, k, v: str, depth: int):
        return v

    def rewrite_dict(self, k, v: Dict, depth: int):
        return v

    def _rewrite_dict(self, k, v: Dict, depth: int):
        new_dict = dict()
        for k_, v_ in v.items():
            if self.discard_item(k_, v_, depth+1, v):
                continue
            try:
                new_dict[k_] = self._rewrite(k_, v_, depth+1) if not k_.startswith('#') else v_
            except StopRewrite:
                pass
        return new_dict

    def rewrite_list(self, k, v: List, depth: int):
        return v

    def _rewrite_list(self, k, v: Union[Tuple, List], depth: int):
        new_list = list()
        for v_ in v:
            try:
                new_list.append(self._rewrite(k, v_, depth+1))
            except StopRewrite:
                pass
        return new_list

    def rewrite_object(self, k, v: Any, depth: int):
        # rewrite "pydantic models" to dict()
        if hasattr(v, 'dict'):
            to_dict = getattr(v, 'dict')
            if inspect.ismethod(to_dict):
                return self.rewrite_dict(k, to_dict(exclude_unset=True), depth)
        return v


class RewriteBaseConf(Rewrite):
    """Create links between nodes"""

    def __init__(self, target: 'Node', links: List['Link']):
        super().__init__(max_depth=2)
        self.target = target
        self.links = links

    def discard_item(self, k, v, depth, parent):
        # discard comments if they exist as keys
        return depth == 1 and k.startswith('#') and k[1:] in parent

    def rewrite_str(self, k, v, depth):
        if depth <= 2 and re.match(r'(@[^@]|&[^&]).*', v):
            m = re.match(r'^([@&])(.+#.+?|.+?)([/|:].*)?$', v)
            if not m:
                crumb = self.target._node_crumb()
                raise FlowError(f'{crumb}: {k}="{v}" is invalid')
            else:
                kind, link, value = m.groups()
                if link == '.':
                    source = self.target
                else:
                    try:
                        source = self.target._find_node(link)
                    except FlowError as e:
                        crumb = self.target._node_crumb()
                        raise NodeNotFoundError(f'{crumb}: {k}="{v}" not found') from e
                link = Link(source, self.target, k, value, kind)
                self.links.append(link)
                return link
        return v


class RewriteNodeConf(Rewrite):
    """Create config that has all the default params set"""

    def __init__(self, func_defs: Dict):
        super().__init__(max_depth=2)
        self.func_defs = func_defs

    def discard_item(self, k, v, depth, parent):
        # discard comments if they exist as function params
        return depth == 1 and k.startswith('#') and k[1:] in self.func_defs

    def rewrite_dict(self, k, v, depth):
        # extend conf with function defaults
        if depth == 0:
            return {**self.func_defs, **v}
        return v


class StopRewriteObjects(Rewrite):

    def rewrite_object(self, k, v, depth):
        raise StopRewrite


class RewriteArgsConf(Rewrite):
    """Create displayable config"""

    def __init__(self, func_defs: Dict):
        super().__init__(max_depth=2)
        self.func_defs = func_defs

    def _json_coerce(self, o):
        return json.loads(json.dumps(StopRewriteObjects().rewrite(o)))

    def discard_item(self, k, v, depth, parent):
        # discard comments and internal args
        if depth == 1 and k.startswith('#') or k.startswith('_'):
            return True
        # discard default args
        if depth == 1 and k in self.func_defs:
            d = self.func_defs[k]
            return v == d or self._json_coerce(v) == self._json_coerce(d)
        return False

    def rewrite_dict(self, k, v, depth):
        return sorted_dict(v)

    def rewrite_object(self, k, v, depth):
        # rewrite links
        if depth <= 2 and isinstance(v, Link):
            link: 'Link' = v
            return str(link)
        return v


class RewriteHashConf(Rewrite):
    """Create hashable config"""

    def discard_item(self, k, v, depth, parent):
        # discard comments and internal args
        return depth == 1 and (k.startswith('#') or k.startswith('_'))

    def rewrite_dict(self, k, v, depth):
        # sort_keys = True
        return sorted_dict(v)

    def rewrite_object(self, k, v, depth):
        # rewrite links including node hash (e.g. @foo.3ed0ab)
        if depth <= 2 and isinstance(v, Link):
            link: 'Link' = v
            value = link.kind
            if link.source == link.target:
                value += '.'
            else:
                value += link.source._slug
            if link.value:
                value += link.value
            return value
        # rewrite objects with hash or system id
        try:
            dump = cloudpickle.dumps(v, protocol=4)
            return hash32(dump)
        except Exception:
            return fake_hash(v)


class RewriteFlowConf(Rewrite):
    """Create dumpable config"""

    def discard_item(self, k, v, depth, parent):
        # discard _agent property
        return depth == 1 and k == '_agent'

    def rewrite_object(self, k, v, depth):
        # rewrite links
        if depth <= 2 and isinstance(v, Link):
            link: 'Link' = v
            return str(link)
        # discard objects
        raise StopRewrite()


class RewriteCallConf(Rewrite):
    """Create callablle config"""

    def __init__(self, funcs: Dict[str, 'Func'], func_pars: Dict, node_vals: Dict['Node', Any]):
        super().__init__(max_depth=2)
        self.funcs = funcs
        self.node_vals = node_vals
        self.func_pars = func_pars

    def discard_item(self, k, v, depth, parent):
        if depth == 1:
            # keep conversion types
            if k.startswith('_type.'):
                return False
            # discard comments and internal args that are no valid function params
            if k.startswith('#') or (k.startswith('_') and k not in self.func_pars):
                return True
        return False

    def rewrite_dict(self, k, v, depth):
        def _coerce(v, name):
            if name:
                if name in self.funcs:
                    func = self.funcs[name]
                else:
                    func = Func.find(name)
                    self.funcs[name] = func
                return func.func(v)
            else:
                return v

        # coerce types
        if depth == 0:
            types = dict()
            for k_, v_ in list(v.items()):
                if k_.startswith('_type.'):
                    types[k_[len('_type.'):]] = v_
                    del v[k_]
            if types:
                return dict((k_, _coerce(v_, types.get(k_, None))) for k_, v_ in v.items())
        return v

    def rewrite_str(self, k, v, depth):
        # rewrite escaped string literals
        if depth <= 2 and re.match(r'(@@|&&|\$\$).+', v):
            return v[1:]
        # rewrite environment variables (fail if not exists)
        if depth == 1 and v.startswith('$'):
            m = re.match(r'^\$[\w]+', v)
            if m:
                name = m.group()[1:]
                return os.environ[name] + v[len(m.group()):]
        # rewrite home directory
        if depth == 1 and v.startswith('~'):
            return os.path.expanduser(v)
        return v

    def rewrite_object(self, k, v, depth):
        # rewrite links with node values
        if depth <= 2 and isinstance(v, Link):
            link: 'Link' = v
            return link.resolve(*self.node_vals.get(link.source, (None, None)))
        return v
