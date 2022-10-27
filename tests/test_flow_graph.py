# type: ignore
import re
from flowfish import flow


def strip_chars(s: str):
    return re.sub(r'\s+', '', s)


def test_dot():
    f = flow({
        'test': {
            'a@dict': {
                '_dump': True
            },
            'b@dict': {
            },
            'c@dict': {
                '_a': '@a',
                'b': '&b'
            }
        }
    })

    g = f.test.c.dot(omit_internal=False)
    assert strip_chars(g.source) == strip_chars(
        """
        digraph Flow {
            graph [splines=true]
            node [fontname=times fontsize=12]
            edge [fontname=times fontsize=10]
            "test.c[dict]" [label=c shape=rect style=rounded]
            "test.a[dict]" [label=a shape=rect style="rounded,filled"]
            "test.b[dict]" [label=b shape=rect style=rounded]
            "test.a[dict]" -> "test.c[dict]" [label=_a]
            "test.b[dict]" -> "test.c[dict]" [label=b arrowhead=odiamond]
            "." [label="" shape=doublecircle]
            "test.c[dict]" -> "."
        }
        """
    )
