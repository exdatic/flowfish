import time

from flowfish.utils import pool_run


def test_pool_run_timing():

    def a(args):
        time.sleep(0.1)
        return 'a'

    def aa(args):
        time.sleep(0.2)
        return 'aa'

    def ab(args):
        time.sleep(0.3)
        return 'ab'

    def aba(args):
        time.sleep(0.4)
        return 'aba'

    def b(args):
        time.sleep(0.5)
        return 'b'

    def c(args):
        time.sleep(0.6)
        return 'c'

    deps = {
        a: [aa, ab],
        ab: aba,
        b: None,
        c: None
    }

    start = time.time()
    assert [
        (r, int(10*(time.time()-start))/10) for r in pool_run(deps)
    ] == [('aa', 0.2), ('aba', 0.4), ('b', 0.5), ('c', 0.6), ('ab', 0.7), ('a', 0.8)]
