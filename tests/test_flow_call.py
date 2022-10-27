# type: ignore
import pytest

from flowfish import flow, FlowError


def test_call_required_args_failure():
    # required argument 'b' is missing
    with pytest.raises(FlowError) as excinfo:
        f = flow({
            'test': {
                'foo@test.function.foo': {
                    'a': 'a',
                    'd': 'd'
                }
            }
        })
        f.test.foo()
    assert str(excinfo.value.__cause__) == "test.function.foo() is missing arguments: ['b']"


def test_call_required_args():
    f = flow({
        'test': {
            'foo@test.function.foo': {
                'a': 'a',
                'b': 'b',
                'd': 'd'
            }
        }
    })
    assert f.test.foo() == ('a', 'b', (), 'd', None, {})


def test_call_required_args_override():
    f = flow({
        'test': {
            'foo@test.function.foo': {
                'a': 'a',
                'b': 'b',
                'd': 'd'
            }
        }
    })
    assert f.test.foo('A', 'B') == ('A', 'B', (), 'd', None, {})


def test_call_varargs_failure():
    # varargs must be list or tuple
    with pytest.raises(FlowError) as excinfo:
        f = flow({
            'test': {
                'foo@test.function.foo': {
                    'a': 'a',
                    'b': 'b',
                    'c': 'c',
                    'd': 'd'
                }
            }
        })
        f.test.foo()
    assert str(excinfo.value.__cause__) == "Invalid varargs: <class 'str'> (must be list)"


def test_call_varargs():
    f = flow({
        'test': {
            'foo@test.function.foo': {
                'a': 'a',
                'b': 'b',
                'c': ('c', 'c'),
                'd': 'd'
            }
        }
    })
    assert f.test.foo() == ('a', 'b', ('c', 'c'), 'd', None, {})


def test_call_varargs_override_posargs():
    f = flow({
        'test': {
            'foo@test.function.foo': {
                'a': 'a',
                'b': 'b',
                'd': 'd'
            }
        }
    })
    assert f.test.foo('A', 'B', 'C', 'C') == ('A', 'B', ('C', 'C'), 'd', None, {})


def test_call_varargs_override_kwargs():
    f = flow({
        'test': {
            'foo@test.function.foo': {
                'a': 'a',
                'b': 'b',
                'd': 'd'
            }
        }
    })
    assert f.test.foo('A', 'B', c=('C', 'C')) == ('A', 'B', ('C', 'C'), 'd', None, {})


def test_call_varargs_override_posargs_and_kwargs():
    # posargs are prefered over kwargs:
    # test.foo(*args, *{**conf, **kwargs})
    f = flow({
        'test': {
            'foo@test.function.foo': {
                'a': 'a',
                'b': 'b',
                'd': 'd'
            }
        }
    })
    assert f.test.foo('A', 'B', 'Z', 'Z', c=('C', 'C')) == ('A', 'B', ('Z', 'Z'), 'd', None, {})


def test_call_underscore_args():
    # underscore args can only be used if they are already valid function args,
    # underscore args in **kwargs are not supported
    f = flow({
        'test': {
            'bar@test.function.bar': {
                'bar': 'bar',
                '_bar': '_bar',
                '_foo': '_foo'
            }
        }
    })
    assert f.test.bar() == ('bar', '_bar', {})


def test_call_byref():
    f = flow({
        'test': {
            'tokenize@test.function.tokenize': {
            },
            'analyzer@test.function.analyzer': {
                'tokenize': '&tokenize',
                'input': 'hello world'
            }
        }
    })

    assert f.test.analyzer() == ['hello', 'world']
