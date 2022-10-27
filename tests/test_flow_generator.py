# type: ignore
from flowfish import flow


def test_call_generator(tmpdir):
    f = flow({
        'test': {
            'numbers@test.function.numbers': {
            }
        }
    }, data_dir=tmpdir)

    assert list(f.test.numbers()) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    # the numbers generator should not be exhausted if called a second time
    assert list(f.test.numbers()) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_call_generator_multiple_times(tmpdir):
    f = flow({
        'test': {
            'numbers@test.function.numbers': {
            },
            'foo@test.function.consume': {
                'iterable': '@numbers'
            },
            'bar@test.function.consume': {
                'iterable': '@numbers'
            },
            'foobar@map': {
                'foo': '@foo',
                'bar': '@bar',
                'input': '',
                'value': '[foo, bar]'
            }
        }
    }, data_dir=tmpdir)

    assert f.test.foobar() == [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]]


def test_call_generator_multiple_times_async(tmpdir):
    f = flow({
        'test': {
            'numbers@test.function.numbers_async': {
            },
            'foo@test.function.consume_async': {
                'aiterable': '@numbers'
            },
            'bar@test.function.consume_async': {
                'aiterable': '@numbers'
            },
            'foobar@map': {
                'foo': '@foo',
                'bar': '@bar',
                'input': '',
                'value': '[foo, bar]'
            }
        }
    }, data_dir=tmpdir)

    assert f.test.foobar() == [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]]
