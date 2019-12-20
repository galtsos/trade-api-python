import asyncio
import os
import signal
from functools import partial
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence

LoopExceptionHandlerCallable = Callable[[asyncio.AbstractEventLoop, Dict[str, Any]], Any]


def signal_handler(sig: signal.Signals, loop: asyncio.AbstractEventLoop) -> None:
    print(f'Received exit signal {sig.name}')
    loop.create_task(shutdown(loop))


# Inspired by https://www.roguelynn.com/words/asyncio-exception-handling/
async def shutdown(loop: asyncio.AbstractEventLoop) -> None:
    """Cleanup tasks tied to the program's shutdown."""
    print('Shutting down...')

    other_tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task(loop)]
    print(f'Cancelling {len(other_tasks)} outstanding tasks')
    await _cancel_tasks(loop, other_tasks)

    loop.stop()


# Do it like asyncio.run()
async def _cancel_tasks(loop: asyncio.AbstractEventLoop, tasks: List) -> None:
    if not tasks:
        return

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True, loop=loop)

    for task in tasks:
        if task.cancelled():
            continue

        if task.exception():
            loop.default_exception_handler({
                'message': 'Unhandled exception during shutdown',
                'exception': task.exception(),
                'task': task,
            })


def run_program_forever(
    target: Callable[..., Awaitable],
    loop: Optional[asyncio.AbstractEventLoop] = None,
    loop_debug: Optional[bool] = None,
    handle_signals: Optional[Sequence[signal.Signals]] = None
) -> None:
    """
    Args:
        - handle_signals: If None then these signals will be handled: SIGTERM, SIGINT.
    """
    if not loop:
        loop = asyncio.new_event_loop()

    if loop_debug is not None:
        loop.set_debug(loop_debug)

    if handle_signals is None:
        handle_signals = {signal.SIGTERM, signal.SIGINT}

    for sig in handle_signals:
        loop.add_signal_handler(sig, partial(signal_handler, sig, loop))

    try:
        env = AsyncProgramEnv()
        loop.set_exception_handler(env.exception_handler)
        loop.create_task(target(env))
        loop.run_forever()
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        print('Successfully shutdown the program.')


class AsyncProgramEnv:
    """
    Main purpose of the class is give an ability to "patch" an event loop exception handler.
    A user can do additional shutdown tasks in the patch.
    """

    def __init__(self):
        self._exception_handler_patch: Optional[LoopExceptionHandlerCallable] = None

    @property
    def exception_handler_patch(self):
        return self._exception_handler_patch

    @exception_handler_patch.setter
    def exception_handler_patch(self, value: Optional[LoopExceptionHandlerCallable] = None):
        if value is not None and not callable(value):
            raise TypeError('Value should be a callable or None')

        self._exception_handler_patch = value

    def exception_handler(self, loop: asyncio.AbstractEventLoop, context: Dict[str, Any]) -> None:
        if self.exception_handler_patch:
            self.exception_handler_patch(loop, context)

        print(f'Caught exception (process #{os.getpid()}):')
        loop.default_exception_handler(context)

        loop.create_task(shutdown(loop))
