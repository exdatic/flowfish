# type: ignore
import pkg_resources

from flowfish import flow


BASE_DIR = pkg_resources.resource_filename(__name__, 'data/')


def test_multiconf():
    f = flow([f'{BASE_DIR}/a.json', f'{BASE_DIR}/b.json'])
    assert str(list(f)) == "[a, b]"


def test_multiconf_override():
    f = flow([f'{BASE_DIR}/a.json', f'{BASE_DIR}/b.json', {'a': {'b@dict': {}}}])
    assert str(list(f.a)) == "[a.b[dict], a.a[dict]]"


def test_multiconf_reference_same_scope():
    f = flow([f'{BASE_DIR}/a.json', f'{BASE_DIR}/b.json', {'c@b': {'c@b': {}}}])
    assert str(list(f.c)) == "[c.c@b[dict], c.b[dict]]"


def test_multiconf_reference_other_scope():
    f = flow([f'{BASE_DIR}/a.json', f'{BASE_DIR}/b.json', {'c@b': {'c@a.a': {}}}])
    assert str(list(f.c)) == "[c.c@a[dict], c.b[dict]]"
