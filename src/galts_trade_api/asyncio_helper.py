import asyncio
import os
import signal
from functools import partial
from typing import AbstractSet, Any, Awaitable, Callable, Dict, List, Optional

from .structlogger import get_logger

logger = get_logger('galts_trade_api')

LoopExceptionHandlerCallable = Callable[[asyncio.AbstractEventLoop, Dict[str, Any]], Any]


def signal_handler(sig: signal.Signals, loop: asyncio.AbstractEventLoop) -> None:
    logger.info('Received exit signal', process_id=os.getpid(), signal=sig.name)
    loop.create_task(shutdown(loop))


# Inspired by https://www.roguelynn.com/words/asyncio-exception-handling/
async def shutdown(loop: asyncio.AbstractEventLoop) -> None:
    """Cleanup tasks tied to the program's shutdown."""
    logger.info(f'Shutting down', process_id=os.getpid())

    other_tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task(loop)]
    logger.debug('Cancelling outstanding tasks', process_id=os.getpid(), count=len(other_tasks))
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
    handle_signals: Optional[AbstractSet[signal.Signals]] = None
) -> None:
    """
    Args:
        handle_signals: If None then these signals will be handled: SIGTERM, SIGINT.
    """
    if not loop:
        loop = asyncio.new_event_loop()
        new_loop_created = True
    else:
        new_loop_created = False

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
        if new_loop_created:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

        logger.info('Successfully shutdown', process_id=os.getpid())


class AsyncProgramEnv:
    """
    The main purpose of the class is to provide an ability to extend the event loop exception
    handler. A user can perform additional shutdown tasks in the extension.
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
        logger.info('Caught exception', process_id=os.getpid())

        if self.exception_handler_patch:
            self.exception_handler_patch(loop, context)
        else:
            loop.default_exception_handler(context)

        loop.create_task(shutdown(loop))
