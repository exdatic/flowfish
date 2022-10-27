# type: ignore
import json
import pickle
from flowfish import flow


def test_node_conf():
    # conf must contain metadata and additional function defaults
    f = flow({
        'test': {
            'foo@test.function.foo': {
                'a': 'a',
                'b': 'b',
                'd': 'd'
            }
        }
    })
    assert f.test.foo.conf == {'test': {'foo': {
        '_base': 'foo',
        '_func': 'test.function.foo',
        '_hash': '6c9cc6b0',
        '_root': True,
        'a': 'a', 'b': 'b', 'd': 'd'
    }}}


def test_node_args():
    # hash conf my not contain metadate
    f = flow({
        'test': {
            'foo@test.function.foo': {
                'a': 'a',
                'b': 'b',
                'd': 'd'
            }
        }
    })
    assert f.test.foo.args == {
        'a': 'a', 'b': 'b', 'd': 'd'
    }


def test_node_hash_conf():
    # hash conf my not contain metadate
    f = flow({
        'test': {
            'foo@test.function.foo': {
                'a': 'a',
                'b': 'b',
                'd': 'd'
            }
        }
    })
    assert f.test.foo._hash_conf == {'foo': {
        'a': 'a', 'b': 'b', 'd': 'd', 'e': None
    }}


def test_node_conf_pos_only():
    f = flow({
        "math": {
            "sum": {
            }
        }
    })
    assert f.math.sum.conf == {'math': {'sum': {
        '_base': 'sum',
        '_func': 'sum',
        '_hash': 'f38765ce',
        '_root': True,
    }}}


def test_node_signature():
    f = flow({
        'test': {
            'foo@test.function.foo': {
                'a': 'a',
                'b': 'b',
                'e': 'e',
                'f': 'f'
            }
        }
    })

    assert str(f.test.foo.__signature__) == "(a='a', b='b', *c, d, e='e', f='f', **kwargs)"


def test_node_doc():
    f = flow({
        'test': {
            'foo@test.function.foo': {
                'a': 'a',
                'b': 'b',
                'e': 'e',
                'f': 'f'
            }
        }
    })

    assert str(f.test.foo.__doc__) == "I am foo"


def test_node_dump(tmpdir):
    f = flow({
        'test': {
            'foobar@test.function.foobar': {
                '_dump': True
            }
        }
    }, data_dir=tmpdir)

    assert f.test.foobar() == "foobar"
    assert f.test.foobar.data == "foobar"
    assert f.test.foobar._cached
    assert f.test.foobar._dumped
    assert f.test.foobar._done
    assert f.test.foobar._data_file.is_file()
    assert f.test.foobar._conf_file.is_file()
    with open(f.test.foobar._data_file, mode='rb') as fp:
        assert pickle.load(fp) == "foobar"
    with open(f.test.foobar._conf_file, mode='r') as fp:
        assert json.load(fp) == f.test.foobar.conf
