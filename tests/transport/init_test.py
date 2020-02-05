import asyncio
from asyncio import Event
from dataclasses import dataclass
from multiprocessing.connection import Pipe
from typing import Any, Dict, Mapping, Optional, Sequence, Type
from unittest.mock import Mock

import pytest

from galts_trade_api.transport import DepthConsumeKey, MessageConsumerCollection, PipeRequest, \
    PipeResponseRouter


def fixture_format_for_rabbitmq_correct():
    yield 'a.b.c', ['a', 'b', 'c'], {}
    yield 'a.b.*', ['a', 'b'], {}
    yield 'a.*.*', ['a'], {}

    yield 'a.*.*', [], {'exchange_tag': 'a'}
    yield '*.b.*', [], {'market_tag': 'b'}
    yield '*.*.c', [], {'symbol_tag': 'c'}

    yield '#', ['*', '*', '*'], {}
    yield '#', [], {'exchange_tag': '*', 'market_tag': '*', 'symbol_tag': '*'}


def fixture_format_for_rabbitmq_wrong():
    yield '', 'b', 'c', 'Field exchange'
    yield 'a', '', 'c', 'Field market_tag'
    yield 'a', 'b', '', 'Field symbol_tag'
    yield '', '', '', 'Field exchange'


class TestDepthConsumeKey:
    @staticmethod
    @pytest.mark.parametrize('expected_result, args, kwargs', fixture_format_for_rabbitmq_correct())
    def test_format_for_rabbitmq_result_format(
        expected_result: str,
        args: Sequence,
        kwargs: Mapping
    ):
        key = DepthConsumeKey(*args, **kwargs)

        assert expected_result == key.format_for_rabbitmq()

    @staticmethod
    @pytest.mark.parametrize(
        'exchange, market_tag, symbol_tag, expected_msg',
        fixture_format_for_rabbitmq_wrong()
    )
    def test_format_for_rabbitmq_exceptions(
        exchange: str,
        market_tag: str,
        symbol_tag: str,
        expected_msg: str
    ):
        with pytest.raises(ValueError, match=expected_msg):
            DepthConsumeKey(exchange, market_tag, symbol_tag).format_for_rabbitmq()


class TestMessageConsumerCollection:
    @staticmethod
    @pytest.mark.asyncio
    async def test_send_will_call_multiple_consumers():
        event1 = Event()
        event2 = Event()
        expected_data = 'test'

        async def consumer1(data):
            nonlocal event1, expected_data
            assert data == expected_data
            event1.set()

        async def consumer2(data):
            nonlocal event2, expected_data
            assert data == expected_data
            event2.set()

        collection = MessageConsumerCollection()
        collection.add_consumer(consumer1)
        collection.add_consumer(consumer2)
        await collection.send(expected_data)

        assert event1.is_set()
        assert event2.is_set()


@dataclass(frozen=True)
class RequestStub(PipeRequest):
    arg: str


def fixture_start_correct():
    expected_response = 'test response'
    request = RequestStub('test')

    yield expected_response, 0.001, request, [request, expected_response]
    yield expected_response, None, request, [request, expected_response]


def fixture_start_exceptions():
    yield 0, ValueError, 'object with indexing'
    yield 'a message', ValueError, 'exactly 2 elements'
    yield [RuntimeError('test')], RuntimeError, 'test'


class TestPipeResponseRouter:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'expected_response, sleep_after_start, pipe_request, response',
        fixture_start_correct()
    )
    async def test_start_correct(
        self,
        expected_response: str,
        sleep_after_start: Optional[float],
        pipe_request: PipeRequest,
        response: Any,
    ):
        parent_conn, child_conn = Pipe()
        consumer_called = Event()
        poll_delay = 0.001

        def on_exception(_: asyncio.AbstractEventLoop, context: Dict[str, Any]) -> None:
            pytest.fail(f"Unexpected exception: {context['exception']}")

        asyncio.get_running_loop().set_exception_handler(on_exception)

        router = PipeResponseRouter(parent_conn, poll_delay)
        router_task = asyncio.create_task(router.start())

        # Give the task time to some work like it will be in real usage
        if sleep_after_start:
            await asyncio.sleep(sleep_after_start)

        async def on_response(data):
            consumer_called.set()
            assert data == expected_response

        consumer_collection = router.prepare_consumers_of_response(pipe_request)
        consumer_collection.add_consumer(on_response)

        child_conn.send(response)

        await asyncio.sleep(poll_delay * 5)

        assert consumer_called.is_set()

        router_task.cancel()

    @pytest.mark.asyncio
    async def test_start_send_only_to_appropriate_consumer(self):
        pipe_request1 = RequestStub('test 1')
        pipe_request2 = RequestStub('test 2')
        response = [pipe_request1, 'test response']

        parent_conn, child_conn = Pipe()
        consumer1_called = Event()
        consumer2_called = Event()
        poll_delay = 0.001

        def on_exception(_: asyncio.AbstractEventLoop, context: Dict[str, Any]) -> None:
            pytest.fail(f"Unexpected exception: {context['exception']}")

        asyncio.get_running_loop().set_exception_handler(on_exception)

        router = PipeResponseRouter(parent_conn, poll_delay)
        router_task = asyncio.create_task(router.start())

        async def on_response1(data): consumer1_called.set()

        async def on_response2(data): consumer1_called.set()

        consumer_collection = router.prepare_consumers_of_response(pipe_request1)
        consumer_collection.add_consumer(on_response1)
        consumer_collection = router.prepare_consumers_of_response(pipe_request2)
        consumer_collection.add_consumer(on_response2)

        child_conn.send(response)

        await asyncio.sleep(poll_delay * 5)

        assert consumer1_called.is_set()
        assert not consumer2_called.is_set()

        router_task.cancel()

    @pytest.mark.asyncio
    async def test_start_dont_dispatch_unknown_request(self):
        pipe_request_known = RequestStub('test 1')
        pipe_request_unknown = RequestStub('test 2')
        response = [pipe_request_unknown, 'test response']

        parent_conn, child_conn = Pipe()
        consumer1_called = Event()
        poll_delay = 0.001

        def on_exception(_: asyncio.AbstractEventLoop, context: Dict[str, Any]) -> None:
            pytest.fail(f"Unexpected exception: {context['exception']}")

        asyncio.get_running_loop().set_exception_handler(on_exception)

        router = PipeResponseRouter(parent_conn, poll_delay)
        router_task = asyncio.create_task(router.start())

        async def on_response(data): consumer1_called.set()

        consumer_collection = router.prepare_consumers_of_response(pipe_request_known)
        consumer_collection.add_consumer(on_response)

        child_conn.send(response)

        await asyncio.sleep(poll_delay * 5)

        assert not consumer1_called.is_set()

        router_task.cancel()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'response, expected_exception_class, expected_exception_msg',
        fixture_start_exceptions()
    )
    async def test_start_exceptions_in_router_task(
        self,
        response: Any,
        expected_exception_class: Optional[Type[Exception]],
        expected_exception_msg: Optional[str]
    ):
        parent_conn, child_conn = Pipe()
        poll_delay = 0.001

        router = PipeResponseRouter(parent_conn, poll_delay)
        router_task = asyncio.create_task(router.start())

        child_conn.send(response)

        await asyncio.sleep(poll_delay * 50)

        with pytest.raises(expected_exception_class, match=expected_exception_msg):
            router_task.result()

    @pytest.mark.asyncio
    async def test_start_exceptions_in_consumer_task(self):
        expected_response = 'test response'
        pipe_request = RequestStub('test')
        response = [pipe_request, expected_response]
        expected_exception = RuntimeError('test exception from consumer coroutine')

        parent_conn, child_conn = Pipe()
        poll_delay = 0.001
        exception = None

        def on_exception(_: asyncio.AbstractEventLoop, context: Dict[str, Any]) -> None:
            nonlocal exception
            exception = context['exception']

        asyncio.get_running_loop().set_exception_handler(on_exception)

        router = PipeResponseRouter(parent_conn, poll_delay)
        router_task = asyncio.create_task(router.start())

        async def on_response(data): raise expected_exception

        consumer_collection = router.prepare_consumers_of_response(pipe_request)
        consumer_collection.add_consumer(on_response)

        child_conn.send(response)

        await asyncio.sleep(poll_delay * 5)

        assert exception is expected_exception
        assert not router_task.cancelled()

        router_task.cancel()

    def test_prepare_consumers_of_response_realize_singleton(self):
        parent_conn = Mock()
        router = PipeResponseRouter(parent_conn)

        pipe_request = RequestStub('test')
        res1 = router.prepare_consumers_of_response(pipe_request)
        res2 = router.prepare_consumers_of_response(pipe_request)

        assert res1 is res2
