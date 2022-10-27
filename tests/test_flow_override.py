# type: ignore
import pkg_resources

from flowfish import flow


BASE_DIR = pkg_resources.resource_filename(__name__, 'data/')


def test_override_crosslink():
    foo_flow = flow(f'{BASE_DIR}/foo.json')
    assert foo_flow.foo.foobar.conf == {
        'foo': {
            'foobar': {
                '_base': 'foobar',
                '_func': 'dict',
                '_hash': '2a855f6e',
                '_root': True,
                'foobar': 'foobar',
                'barfoo': 'barfoo'}}}

    assert foo_flow.foo.bar.conf == {
        'foo': {
            'bar': {
                '_base': 'bar',
                '_func': 'dict',
                '_hash': 'eda7c634',
                '_root': True,
                'foo': 'foo',
                'bar': 'bar'}}}

    bar_flow = foo_flow.foo.bar._base_node._flow
    assert bar_flow.bar.foo.conf == {
        'bar': {
            'foo': {
                '_base': 'foo',
                '_func': 'dict',
                '_hash': '5a97d7da',
                '_root': True,
                'foo': 'foo',
                'bar': 'bar'}}}


def test_override_reassign():
    f = flow({
        'torch': {
            '_props': {
                'embeddable_model.embeddable': True
            },
            'model@dict': {
                'name': 'good_model',
                '1': 1
            },
            'pretrained_model@model': {
                'pretrained': True,
                '2': 2
            },
            'embeddable_model@pretrained_model': {
                '3': 3
            }
        },
        'torch_custom@torch': {
            'model@dict': {
                'name': 'best_model',
                '4': 4
            }
        }
    })

    assert f.torch.model.args == {
        'name': 'good_model',
        '1': 1
    }
    assert f.torch.pretrained_model.args == {
        'name': 'good_model',
        'pretrained': True,
        '1': 1,
        '2': 2
    }
    assert f.torch.embeddable_model.args == {
        'name': 'good_model',
        'pretrained': True,
        'embeddable': True,
        '1': 1,
        '2': 2,
        '3': 3
    }
    assert f.torch_custom.model.args == {
        'name': 'best_model',
        '4': 4
    }
    assert f.torch_custom.pretrained_model.args == {
        'name': 'best_model',
        'pretrained': True,
        '4': 4,
        '2': 2
    }
    assert f.torch_custom.embeddable_model.args == {
        'name': 'best_model',
        'pretrained': True,
        'embeddable': True,
        '4': 4,
        '2': 2,
        '3': 3
    }


def test_override_sideload():
    dog1 = flow(f'{BASE_DIR}/dog1.json')
    dog2 = flow(f'{BASE_DIR}/dog2.json')
    dog3 = flow(f'{BASE_DIR}/dog3.json')
    assert dog1.dog.love() == {'love': 'barf'}
    assert dog2.dog.love() == {'hate': 'lime'}
    assert dog3.dog.love() == {'hate': 'lime'}


def test_override_crosslink_foobar():
    f = flow(f'{BASE_DIR}/foobar.json')
    f.foo.foo()
    f.foo.bar()
    f.bar.foo()
    f.bar.bar()


def test_override_crosslink_foobar2():
    f = flow(f'{BASE_DIR}/foobar2.json')
    f.bar.foo()
    f.bar.bar()
