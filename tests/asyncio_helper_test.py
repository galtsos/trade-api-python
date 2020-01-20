import asyncio
from asyncio import Event
from unittest.mock import ANY, Mock

import pytest

from galts_trade_api.asyncio_helper import AsyncProgramEnv
from .utils import AsyncMock


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
