import asyncio
from asyncio.streams import StreamReader, StreamWriter
import codecs
import functools
from io import TextIOBase
import signal
import sys
import threading
from typing import Callable, Coroutine, IO, Optional, TextIO


class Buffer(TextIOBase, TextIO):
    """"Carriage return" aware line buffer"""

    def __init__(self, max_lines: int = -1):
        self.lines = []
        self.max_lines = max_lines

    def __call__(self, s: str):
        self.write(s)

    def __iter__(self):
        yield from self.lines

    @property
    def encoding(self):
        return 'utf-8'

    def writable(self):
        return True

    def write(self, s: str):
        # merge value with last open line
        offs = 0
        if self.lines and self.lines[-1][-1] != '/n':
            last = self.lines.pop()
            offs = max(0, len(last) - 1)
            s = last + s

        start, end, size = 0, 0, len(s)

        while start < size:
            end = s.find('\n', start + offs)
            if end == -1:
                end = size
                is_last = True
                is_open = True
            else:
                # include line feed
                end = end + 1
                is_last = end == size
                is_open = False

            # discard everything below last carriage return
            if is_last and is_open:
                pos = s.rfind('\r', start + offs, end - 1)
            else:
                pos = s.rfind('\r', start + offs, end)

            if pos != -1:
                start = pos + 1

            if end - start > 0:
                self.lines.append(s[start:end])

            start = end
            offs = 0

        if self.max_lines > 0:
            self.lines = self.lines[-self.max_lines:]

    def __repr__(self):
        return ''.join(self.lines)

    def __len__(self):
        return sum(len(line) for line in self.lines)


def run_async_threaded(func: Callable[[], Coroutine], loop_policy=None):
    """Same as ``await func()``, but threaded"""

    def start():
        asyncio.set_event_loop(loop)
        loop.run_forever()

    async def stop():
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        loop.stop()

    # create loop
    if loop_policy:
        loop = loop_policy.new_event_loop()
    else:
        loop = asyncio.new_event_loop()

    for s in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(s, lambda: asyncio.run_coroutine_threadsafe(stop(), loop))

    # start thread and run loop
    thread = threading.Thread(target=start, daemon=True)
    thread.start()

    # run task in thread's loop
    future = asyncio.run_coroutine_threadsafe(func(), loop)
    future.add_done_callback(lambda f: asyncio.create_task(stop()))

    thread.join()
    loop.close()

    return future.result()


def run_async(func: Callable[[], Coroutine], loop_policy=None, use_thread: Optional[bool] = None):
    """Same as ``await func()``"""
    if use_thread is None:
        # use thread if called from main thread
        use_thread = threading.current_thread() is threading.main_thread()

    if use_thread:
        return run_async_threaded(func, loop_policy=loop_policy)
    else:
        if loop_policy:
            loop = loop_policy.new_event_loop()
        else:
            loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(func())
        finally:
            loop.close()


async def async_subprocess(
    *cmd,
    stdin: Optional[IO] = None,
    stdout: Optional[IO] = None,
    stderr: Optional[IO] = None,
    read_limit=2**16, shell=False
):
    """
    Execute subprocess asynchronously

    Issues
    ------
    Python Bug 35621: asyncio.create_subprocess_exec() only works with main event loop (fixed in 3.8)
    """

    async def read(stream: StreamWriter, reader: IO):
        if isinstance(reader, TextIOBase):
            encode = codecs.getincrementalencoder(reader.encoding or 'utf-8')(errors='replace').encode
        else:
            encode = None
        while True:
            chunk = reader.read(read_limit)
            if encode:
                value = encode(chunk)
            else:
                value = chunk
            if value:
                stream.write(value)  # type: ignore
            if not chunk:
                break
        try:
            await stream.drain()
        finally:
            stream.close()

    async def write(stream: StreamReader, writer: IO):
        if isinstance(writer, TextIOBase):
            decode = codecs.getincrementaldecoder(writer.encoding or 'utf-8')(errors='replace').decode
        else:
            decode = None
        while True:
            chunk = await stream.read(read_limit)
            if decode:
                value = decode(chunk)
            else:
                value = chunk
            if value:
                writer.write(value)
                # workaround problem in carriage return handling in IPython
                if hasattr(writer, '_flush_pending'):
                    while writer._flush_pending:  # type: ignore
                        await asyncio.sleep(0)
            if not chunk:
                break

    if shell:
        # TODO replace naive str.join() by shlex.join() from 3.8
        process = await asyncio.create_subprocess_shell(
            ' '.join(cmd),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=read_limit)
    else:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=read_limit)

    # default to standard streams
    stdout = sys.stdout if stdout is None else stdout
    stderr = sys.stderr if stderr is None else stderr

    tasks = []
    if process.stdout:
        tasks.append(write(process.stdout, stdout))
    if process.stderr:
        tasks.append(write(process.stderr, stderr))
    if process.stdin and stdin is not None:
        tasks.append(read(process.stdin, stdin))

    await asyncio.wait(tasks)
    return await process.wait()


def subprocess(*cmd,
               stdin: Optional[IO] = None,
               stdout: Optional[IO] = None,
               stderr: Optional[IO] = None,
               read_limit=2**16, shell=False):
    try:
        # Setting a loop_policy workarounds Python Bug 35621 (fixed in 3.8):
        # asyncio.create_subprocess_exec() only works with main event loop
        import uvloop
        loop_policy = uvloop.EventLoopPolicy()
    except ImportError:
        loop_policy = asyncio.get_event_loop_policy()
    return run_async(functools.partial(
        async_subprocess, *cmd,
        stdin=stdin,
        stdout=stdout,
        stderr=stderr,
        read_limit=read_limit,
        shell=shell
    ), loop_policy=loop_policy)
