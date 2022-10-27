import asyncio
from pydantic import BaseModel
from typing import Optional, Set, Tuple


def foo(a, b, *c, d, e=None, **kwargs):
    """I am foo"""
    return a, b, c, d, e, kwargs


def bar(bar, _bar, **kwargs):
    """I am bar"""
    return bar, _bar, kwargs


def foobar():
    return 'foobar'


async def async_foobar():
    await asyncio.sleep(0)
    return 'foobar'


def tokenize(input):
    return input.split()


def analyzer(tokenize, input):
    return tokenize(input)


def numbers():
    for i in range(10):
        yield i


async def numbers_async():
    for i in range(10):
        yield i


def consume(iterable):
    return [i for i in iterable]


async def consume_async(aiterable):
    return [i async for i in aiterable]


def coerce_list(a_set: Set, a_tuple: Tuple):
    return a_set, a_tuple


class Foobar(BaseModel):
    foo: str
    bar: Optional[str]


def coerce_pydantic(model: 'Foobar'):
    return model
