# type: ignore
from flowfish import flow


def test_coerce_set():
    f = flow({
        'test': {
            'coerce@test.function.coerce_list': {
                'a_set': [1, 2, 3],
                'a_tuple': [1, 2, 3]
            }
        }
    })

    assert f.test.coerce() == ({1, 2, 3}, (1, 2, 3))


def test_coerce_pydantic():
    f = flow({
        'test': {
            'coerce@test.function.coerce_pydantic': {
                'model': {'foo': 'foo'}
            }
        }
    })

    result_model = f.test.coerce()
    assert repr(result_model) == "Foobar(foo='foo', bar=None)"
    assert f.test.coerce.args == {'model': {'foo': 'foo'}}

    f = flow({
        'test': {
            'coerce@test.function.coerce_pydantic': {
            }
        }
    }, {
        'test.coerce.model': result_model
    })

    result_model = f.test.coerce()
    assert repr(result_model) == "Foobar(foo='foo', bar=None)"

    # if exclude_unset=True, bar=None should be omitted
    assert f.test.coerce.args == {'model': {'foo': 'foo'}}
