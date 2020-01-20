import asyncio
import signal
from asyncio import Event
from unittest.mock import ANY, Mock

import pytest

from galts_trade_api.asyncio_helper import AsyncProgramEnv, run_program_forever, signal_handler
from .utils import AsyncMock


def test_signal_handler_log_and_shutdown(mocker):
    logger_mock = mocker.patch('galts_trade_api.asyncio_helper.logger')
    shutdown_mock = mocker.patch('galts_trade_api.asyncio_helper.shutdown')
    loop = Mock(spec_set=asyncio.AbstractEventLoop)

    signal_handler(signal.SIGTERM, loop)

    loop.create_task.assert_called_once()
    shutdown_mock.assert_called_once_with(loop)
    logger_mock.info.assert_called_once_with(
        'Received exit signal',
        process_id=ANY,
        signal='SIGTERM'
    )


def fixture_setup_loop_by_arguments():
    yield True, [signal.SIGTERM]
    yield False, [signal.SIGABRT]


class TestRunProgramForever:
    @pytest.mark.parametrize(
        'loop_debug, handle_signals',
        fixture_setup_loop_by_arguments()
    )
    def test_setup_loop_by_arguments(self, mocker, loop_debug, handle_signals):
        logger_mock = mocker.patch('galts_trade_api.asyncio_helper.logger')
        env_mock = mocker.patch('galts_trade_api.asyncio_helper.AsyncProgramEnv')
        loop = Mock(spec_set=asyncio.AbstractEventLoop)

        def program(env):
            assert env is env_mock()

        run_program_forever(program, loop, loop_debug, handle_signals)

        loop.set_debug.assert_called_once_with(loop_debug)

        for sig in handle_signals:
            loop.add_signal_handler.assert_any_call(sig, ANY)

        loop.set_exception_handler.assert_called_once_with(env_mock().exception_handler)
        loop.create_task.assert_called_once_with(program(env_mock()))
        loop.run_forever.assert_called_once_with()
        logger_mock.info.assert_called_once_with('Successfully shutdown', process_id=ANY)

    def test_setup_loop_by_defaults(self, mocker):
        mocker.patch('galts_trade_api.asyncio_helper.AsyncProgramEnv')
        new_event_loop_mock = mocker.patch('asyncio.new_event_loop', autospec=True)

        run_program_forever(lambda e: None)

        loop = new_event_loop_mock()
        loop.set_debug.assert_not_called()
        loop.add_signal_handler.assert_any_call(signal.SIGTERM, ANY)
        loop.add_signal_handler.assert_any_call(signal.SIGINT, ANY)

        loop.run_until_complete.assert_called_once_with(loop.shutdown_asyncgens())
        loop.close.assert_called_once_with()


def fixture_exception_handler_patch_success():
    yield None
    yield lambda: None


def fixture_exception_handler_patch_exception_for_wrong_type():
    yield 1
    yield 2.0
    yield 'test'


class TestAsyncProgramEnv:
    def test_constructor_dont_init_patch(self):
        env = AsyncProgramEnv()

        assert env.exception_handler_patch is None

    @pytest.mark.parametrize('value', fixture_exception_handler_patch_success())
    def test_exception_handler_patch_success(self, value):
        env = AsyncProgramEnv()

        env.exception_handler_patch = value

        assert env.exception_handler_patch is value

    @pytest.mark.parametrize('value', fixture_exception_handler_patch_exception_for_wrong_type())
    def test_exception_handler_patch_exception_for_wrong_type(self, value):
        env = AsyncProgramEnv()

        with pytest.raises(TypeError, match='should be a callable or None'):
            env.exception_handler_patch = value

    @pytest.mark.asyncio
    async def test_exception_handler_log_and_shutdown(self, mocker):
        logger_mock = mocker.patch('galts_trade_api.asyncio_helper.logger')
        shutdown_mock = mocker.patch(
            'galts_trade_api.asyncio_helper.shutdown',
            new_callable=AsyncMock
        )
        loop = asyncio.get_running_loop()
        mocker.patch.object(loop, 'default_exception_handler')
        context = {'test': 100}

        env = AsyncProgramEnv()
        env.exception_handler(loop, context)
        await asyncio.sleep(0.001)

        loop.default_exception_handler.assert_called_once_with(context)
        shutdown_mock.assert_called_once_with(loop)
        logger_mock.info.assert_called_once_with('Caught exception', process_id=ANY)

    @pytest.mark.asyncio
    async def test_exception_handler_call_patch(self, mocker):
        mocker.patch('galts_trade_api.asyncio_helper.shutdown')
        is_called = Event()

        def patch(local_loop, local_context):
            is_called.set()
            assert local_loop is loop
            assert local_context is context

        loop = Mock(spec_set=asyncio.AbstractEventLoop)
        context = {}
        env = AsyncProgramEnv()
        env.exception_handler_patch = patch
        env.exception_handler(loop, context)

        assert is_called.is_set()
