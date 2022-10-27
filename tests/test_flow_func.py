# type: ignore
import pytest

from flowfish import flow, FlowError


def test_func_dollar():
    f = flow({'test': {'foobar@test.function$func_name': {'func_name': 'foobar'}}})
    assert f.test.foobar() == 'foobar'


def test_func_builtins():
    f = flow({'test': {'dict': {'a': 'a', 'b': 'b'}}})
    assert f.test.dict() == dict(a='a', b='b')


def test_func_builtins_override():
    # dict: *kwargs override *args
    f = flow({'test': {'dict': {'a': 'a', 'b': 'b'}}})
    assert f.test.dict((('a', 'A'), ('c', 'C'))) == dict(a='a', b='b', c='C')


def test_func_map():
    f = flow({'test': {'baz@map': {}}})
    # required argument 'input' is missing
    with pytest.raises(FlowError) as excinfo:
        f.test.baz()
    assert str(excinfo.value.__cause__) == "map() is missing arguments: ['input']"


def test_func_map_builtins():
    f = flow({'test': {'unique_reverse@map': {
        'input': [1, 2, 3, 2, 1],
        'value': 'list(reversed(list(set([i for i in sorted(input)]))))'
    }}})
    f.test.unique_reverse() == [3, 2, 1]


def test_func_map_params():
    f = flow({
        "test": {
            "path@pathlib.Path": {
            },
            "test_split@map": {
                "input": "foo/bar",
                "value": "path(input).name",
                "path": "&path"
            }
        }
    })
    assert f.test.test_split() == 'bar'


def test_func_run():
    f = flow({
        'test': {
            'id@run': {
                '_cmd': 'id',
                '_args': [{'user': '.'}],
                'user': 'root'
            }
        }
    })
    assert f.test.id() == 0
