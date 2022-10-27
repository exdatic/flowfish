from typing import Callable, Dict, KeysView, List, Optional, Set, Tuple, TYPE_CHECKING, Union

if TYPE_CHECKING:
    import graphviz
    from flowfish.node import Node
    from flowfish.scope import Scope
    from flowfish.link import Link


def dot(
    graph: 'Graph', node: Optional['Node'] = None, direction=1,
    until_done=False, omit_internal=False, scopes: Optional[Set['Scope']] = None
) -> 'graphviz.Digraph':

    try:
        import graphviz
    except ImportError:
        raise ImportError("graphviz not installed: pip install graphviz")

    nodes, links = graph.tree(node, direction, until_done, omit_internal)

    if scopes:
        links = [link for link in links if link.source._scope in scopes or link.target._scope in scopes]
        edges = set(link.source for link in links) | set(link.target for link in links)
        nodes = [n for n in nodes if n._scope in scopes or n in edges]

    g = graphviz.Digraph('Flow', format='svg',
                         graph_attr={'splines': 'true'},
                         node_attr={'fontname': 'times', 'fontsize': '12'},
                         edge_attr={'fontname': 'times', 'fontsize': '10'})
    for n in nodes:
        node_style = ['rounded']
        if n._dumpable:
            node_style.append('filled')
        if n._done:
            node_style.append('bold')

        g.node(repr(n), label=n.name, shape='rect', style=','.join(node_style))

    for link in links:
        edge_attr = dict()
        if not link.source._cachable:
            edge_attr['arrowhead'] = 'onormal'
        if link.kind == '&':
            edge_attr['arrowhead'] = 'odiamond'
        if link.source.scope != link.target.scope:
            edge_attr['dir'] = 'both'
            edge_attr['arrowtail'] = 'dot'

        g.edge(repr(link.source), repr(link.target), label=link.name, **edge_attr)

    # draw start/end node
    if node:
        if direction == 1:
            g.node('.', label='', shape='doublecircle')
            g.edge('.', repr(node))
        elif direction == -1:
            g.node('.', label='', shape='doublecircle')
            g.edge(repr(node), '.')

    return g


class Graph:

    _nodes: Dict['Node', None]  # keep node order
    _outgoing: Dict['Node', Dict['Link', None]]  # keep link order
    _incoming: Dict['Node', Dict['Link', None]]  # keep link order

    def __init__(self):
        self._nodes = dict()
        self._outgoing = dict()
        self._incoming = dict()

    def nodes(self) -> KeysView['Node']:
        return self._nodes.keys()

    def add_node(self, node: 'Node'):
        self._nodes[node] = None

    def add_link(self, link: 'Link'):
        if link.source == link.target:
            raise RecursionError(f'Link failed: {link} (self reference)')

        self._nodes[link.source] = None
        if link.source not in self._outgoing:
            self._outgoing[link.source] = dict()
        self._outgoing[link.source][link] = None

        self._nodes[link.target] = None
        if link.target not in self._incoming:
            self._incoming[link.target] = dict()
        self._incoming[link.target][link] = None

    def tree(self, node: Optional['Node'] = None, direction=1,
             until_done: Union[bool, Callable[['Node'], bool]] = False,
             omit_internal: bool = False
             ) -> Tuple[List['Node'], List['Link']]:

        return self._tree(node, direction, until_done, omit_internal, dict(), dict())

    def _tree(self, node: Optional['Node'], direction,
              until_done, omit_internal,
              nodes: Dict['Node', None],
              links: Dict['Link', None], branch=[]) -> Tuple[List['Node'], List['Link']]:
        if node:
            nodes[node] = None
            if callable(until_done):
                done = until_done(node)
            else:
                done = until_done and node._done
            if not done:
                if not direction or direction == 1:
                    branch = branch + [node]
                    if node in self._outgoing:
                        for link in self._outgoing[node]:
                            if omit_internal and link.internal:
                                continue
                            target = link.target
                            if target in branch:
                                loop = map(lambda n: f'{{{n}}}' if target == n else f'{n}',
                                           branch + [target])
                                raise RecursionError(f'loop detected: {" -> ".join(loop)}')
                            links[link] = None
                            self._tree(target, 1, until_done, omit_internal, nodes, links, branch)
                if not direction or direction == -1:
                    branch = branch + [node]
                    if node in self._incoming:
                        for link in self._incoming[node]:
                            if omit_internal and link.internal:
                                continue
                            source = link.source
                            if source in branch:
                                loop = map(lambda n: f'{{{n}}}' if source == n else f'{n}',
                                           reversed(branch + [source]))
                                raise RecursionError(f'loop detected: {" <- ".join(loop)}')
                            links[link] = None
                            self._tree(source, -1, until_done, omit_internal, nodes, links, branch)
        else:
            # from root node
            if not direction or direction == 1:
                for node in self._nodes:
                    if node not in self._incoming:
                        self._tree(node, direction, until_done, omit_internal, nodes, links, branch)
        return list(nodes), list(links)
