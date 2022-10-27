import os
from pathlib import Path
from typing import TYPE_CHECKING

from flowfish.builtins import map_simpleeval

if TYPE_CHECKING:
    from flowfish.node import Node


class Link:

    def __init__(self, source: 'Node', target: 'Node', param: str, value: str, kind: str):
        self.source = source
        self.target = target
        self.param = param
        self.value = value
        self.kind = kind

    @property
    def internal(self):
        return self.param.startswith('_')

    @property
    def name(self):
        return self.param

    def __repr__(self):
        link = self
        value = link.kind
        if link.source == link.target:
            value += '.'
        elif link.source._flow != link.target._flow:
            # create inter-flow link
            path = link.source._flow._file
            assert path is not None, 'flow link source requires file'
            for base_dir in (link.source._flux.data_dir, Path.cwd()):
                if base_dir in path.parents:
                    path = path.relative_to(base_dir)
                    break
            value += f'{path}#{link.source.scope}.{link.source.name}'
        else:
            # create intra-flow link
            if link.source.scope != link.target.scope:
                value += f'{link.source.scope}.{link.source.name}'
            else:
                value += f'{link.source.name}'
        if link.value:
            value += link.value
        return value

    # TODO refactor to function
    def resolve(self, value, ref):
        if self.kind in ('@', '&'):
            input = value if self.kind == '@' else ref
            # target: "@source/"
            if self.value and self.value.startswith('/'):
                path = self.value[1:]
                # target: "@source/."
                if path == '.':
                    path = value
                elif path.startswith('.'):
                    path = path[1:]
                    # target: "@source/.:"
                    if path.startswith(':'):
                        path = map_simpleeval(input, path[1:])
                        assert isinstance(path, str), 'expression must return a string'
                os.makedirs(self.source._work_dir, exist_ok=True)
                return str(self.source._work_dir / path)
            # target: "@source:"
            if self.value and self.value.startswith(':'):
                return map_simpleeval(input, self.value[1:])
            return input
        else:
            raise ValueError(f'unknown assignment: {repr(self)}')
