# type: ignore
import pytest

from flowfish import flow


def test_loop_detection_root():
    with pytest.raises(RecursionError) as excinfo:
        flow({
            'test': {
                'x@dict': {
                    'x': '@a'
                },
                'a@dict': {
                    'a': '@b'
                },
                'b@dict': {
                    'b': '@a'
                }
            }
        })
    assert str(excinfo.value) == 'Loop detected: test.x @ [test.a] @ test.b @ [test.a]'


def test_loop_detection_cycle():
    with pytest.raises(RecursionError) as excinfo:
        flow({
            'test': {
                'a@dict': {
                    'a': '@b'
                },
                'b@dict': {
                    'b': '@a'
                }
            }
        })
    assert str(excinfo.value) == 'Loop detected: [test.a] @ test.b @ [test.a]'
