# type: ignore
from flowfish import flow


def test_props_flow():
    f = flow({'test': {'foo@test.function.foo': {}}})
    assert (f + {
        'test.foo.a': 'A',
        'test.foo.b': 'B',
        'test.foo.d': 'D'
    }).test.foo() == ('A', 'B', (), 'D', None, {})


def test_props_scope():
    f = flow({'test': {'foo@test.function.foo': {}}})
    assert (f.test + {
        'foo.a': 'A',
        'foo.b': 'B',
        'foo.d': 'D'
    }).foo() == ('A', 'B', (), 'D', None, {})


def test_props_node():
    f = flow({'test': {'foo@test.function.foo': {}}})
    assert (f.test.foo + {
        'a': 'A',
        'b': 'B',
        'd': 'D'
    })() == ('A', 'B', (), 'D', None, {})


def test_props_scope_reassign_node():
    f = flow({
        'test': {
            'foo@test.function.foo': {}
        }
    })
    assert (f.test + {'foo': '@test.function.foobar'}).foo() == 'foobar'
