# type: ignore
from flowfish import flow


def test_assign():
    f = flow({
        'test': {
            'bar@dict': {
            },
            'foo@dict': {
                'bar': '@bar'
            }
        }
    })

    assert f.test.foo() == {'bar': {}}


def test_assign_map():
    f = flow({
        'test': {
            'source@dict': {
                'color': 'red'
            },
            'target@dict': {
                'color': '@source:input.color'
            }
        }
    })

    assert f.test.target() == {'color': 'red'}


def test_assign_map_listcomp():
    f = flow({
        'test': {
            'source@dict': {
                'preds': [[1, 2, 3, 4], [[1, 2], [2, 3], [3, 4], [4]]]
            },
            'target@dict': {
                'y_true': '@source:input.preds[0]',
                'y_pred': '@source:[i[0] for i in input.preds[1]]'
            }
        }
    })

    assert f.test.target() == {'y_pred': [1, 2, 3, 4], 'y_true': [1, 2, 3, 4]}


def test_assign_map_ref():
    f = flow({
        'test': {
            'source@dict': {
                'color': 'red'
            },
            'target@dict': {
                'color': '&source:input().color'
            }
        }
    })

    assert f.test.target() == {'color': 'red'}


def test_assign_path_dotonly(tmpdir):
    f = flow({
        'test': {
            'source@map': {
                'input': 'red'
            },
            'target@dict': {
                'color': '@source/.'
            }
        }
    }, data_dir=tmpdir)

    assert f.test.target() == {'color': f'{f.test.source._work_dir}/red'}


def test_assign_path_map(tmpdir):
    f = flow({
        'test': {
            'source@dict': {
                'color': 'red'
            },
            'target@dict': {
                'color': '@source/.:input.color'
            }
        }
    }, data_dir=tmpdir)

    assert f.test.target() == {'color': f'{f.test.source._work_dir}/red'}
