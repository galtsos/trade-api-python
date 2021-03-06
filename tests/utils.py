import ast
import asyncio
import inspect
import sys
import time
from contextlib import contextmanager
from threading import Event, Thread
from time import perf_counter
from typing import Any, Callable, Dict, Generator
from unittest.mock import MagicMock

import grpc

if sys.version_info >= (3, 8):
    from unittest.mock import AsyncMock
else:
    # For simple testing of async code in Python before 3.8
    # Got from https://stackoverflow.com/a/32498408/3155344
    class AsyncMock(MagicMock):
        async def __call__(self, *args, **kwargs):
            return super(AsyncMock, self).__call__(*args, **kwargs)


def wait_for_result(
    function: Callable,
    expected_result: Any = True,
    strict: bool = True,
    timeout: float = 1,
    delay: float = 0.01
) -> bool:
    threshold_waiting_time = perf_counter() + timeout
    while perf_counter() < threshold_waiting_time:
        result = function()

        if result is expected_result or (not strict and result == expected_result):
            return True

        time.sleep(delay)

    return False


@contextmanager
def thread_with_timeout(target: Callable, timeout: float = 1) -> Generator:
    is_finished = Event()
    exception = None

    def wrapper(is_finished_event: Event) -> None:
        nonlocal exception
        try:
            target()
        except Exception as e:
            exception = e
            sys.exit(1)
        finally:
            is_finished_event.set()

    Thread(target=wrapper, args=(is_finished,), daemon=True).start()

    yield

    if not is_finished.wait(timeout):
        raise RuntimeError(f'Timeout exceeded: {timeout} sec')

    if exception:
        raise exception


def get_decorators(cls) -> Dict:
    """Inspired by: https://stackoverflow.com/a/31197273"""
    target = cls
    decorators = {}

    def visit_function_def(node) -> None:
        decorators[node.name] = []
        for n in node.decorator_list:
            if isinstance(n, ast.Call):
                name = n.func.attr if isinstance(n.func, ast.Attribute) else n.func.id
            else:
                name = n.attr if isinstance(n, ast.Attribute) else n.id

            decorators[node.name].append(name)

    node_iter = ast.NodeVisitor()
    node_iter.visit_FunctionDef = visit_function_def
    node_iter.visit(ast.parse(inspect.getsource(target)))

    return decorators


def fixture_grpc_error_status_codes(*args) -> Generator:
    error_status_codes = [
        grpc.StatusCode.UNKNOWN,
        grpc.StatusCode.DEADLINE_EXCEEDED
    ]

    for status_code in error_status_codes:
        yield (status_code, *args)


def cancel_other_async_tasks() -> None:
    """For async loop clean-up purposes"""
    for t in asyncio.all_tasks():
        if t is not asyncio.current_task():
            t.cancel()
