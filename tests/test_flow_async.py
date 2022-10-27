import asyncio
import contextvars
import functools
import pytest

from flowfish import flow


# backwards compatible with python 3.7 and 3.8
async def to_thread(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)


def test_async():
    f = flow({
        'test': {
            'foobar@test.function.async_foobar': {}
        }
    })

    assert f.test.foobar() == "foobar"  # type: ignore


@pytest.mark.asyncio
async def test_async_from_running_loop():
    f = flow({
        'test': {
            'foobar@test.function.async_foobar': {}
        }
    }, loop=asyncio.get_running_loop())

    # if the function is called in the conventional way with
    # f.test.foobar(), it will cause a deadlock!
    assert await to_thread(f.test.foobar) == "foobar"  # type: ignore
