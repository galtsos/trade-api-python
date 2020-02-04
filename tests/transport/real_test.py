import asyncio
from multiprocessing import Event, Pipe
from multiprocessing.connection import Connection
from typing import Any, AsyncGenerator, Callable, Dict, FrozenSet, Mapping, Sequence
from unittest.mock import ANY, Mock

import aio_pika
import pytest
from pytest_mock import MockFixture

from galts_trade_api.asyncio_helper import AsyncProgramEnv
from galts_trade_api.transport import DepthConsumeKey, PipeRequest, TransportFactoryException
from galts_trade_api.transport.real import ConsumePriceDepthRequest, \
    GetExchangeEntitiesRequest, RabbitConnection, RabbitConsumer, RealTransportFactory, \
    RealTransportProcess
from ..utils import AsyncMock, cancel_other_async_tasks


@pytest.fixture
async def unexpected_exceptions_handler() -> AsyncGenerator[None, None]:
    def on_exception(_, context: Dict[str, Any]) -> None:
        pytest.fail(f"Unexpected exception: {context['message']}")

    loop = asyncio.get_running_loop()

    old_handler = loop.get_exception_handler()
    loop.set_exception_handler(on_exception)
    yield
    loop.set_exception_handler(old_handler)


class TestRabbitConnection:
    def test_constructor_is_singleton(self):
        conn_a = RabbitConnection('test1.local')
        conn_b = RabbitConnection('test1.local')
        conn_c = RabbitConnection('test2.local')

        assert conn_a is conn_b
        assert conn_a is not conn_c

    def test_constructor_dont_init_connection_object(self):
        conn = RabbitConnection('test3.local')

        assert conn.connection is None

    @pytest.mark.asyncio
    async def test_constructor_trim_dsn(self, mocker: MockFixture):
        dsn = ' \t test4.local  '
        expected_dsn = dsn.strip()
        assert dsn != expected_dsn

        connect_robust = mocker.patch('aio_pika.connect_robust', new_callable=AsyncMock)

        conn = RabbitConnection(dsn)
        await conn.create_channel()

        connect_robust.assert_called_once_with(expected_dsn)

    @pytest.mark.asyncio
    async def test_create_channel_reuse_one_connection(self, mocker: MockFixture):
        mocker.patch('aio_pika.connect_robust', new_callable=AsyncMock)

        conn = RabbitConnection('test5.local')
        await conn.create_channel()
        first_execution_connection = conn.connection
        await conn.create_channel()

        assert conn.connection is first_execution_connection

    @pytest.mark.asyncio
    async def test_create_channel_use_delivered_dsn(self, mocker: MockFixture):
        connect_robust = mocker.patch('aio_pika.connect_robust', new_callable=AsyncMock)

        dsn = 'test6.local'
        conn = RabbitConnection(dsn)
        prefetch_count = 50

        result = await conn.create_channel(prefetch_count=prefetch_count)

        connect_robust.assert_called_once_with(dsn)
        result.set_qos.assert_called_once_with(prefetch_count=prefetch_count)


class TestRabbitConsumer:
    def test_constructor_trim_exchange_name(self):
        exchange_name = ' \t test.local  '
        expected_exchange_name = exchange_name.strip()
        assert exchange_name != expected_exchange_name

        channel = Mock(spec_set=aio_pika.Channel)
        consumer = RabbitConsumer(channel, exchange_name, lambda: None)

        assert consumer.exchange_name == expected_exchange_name

    @pytest.mark.asyncio
    async def test_create_queue_use_delivered_exchange_name(self):
        channel = AsyncMock(spec_set=aio_pika.Channel)
        exchange_name = 'an_exchange'

        def cb(): pass

        consumer = RabbitConsumer(channel, exchange_name, cb)

        result = await consumer.create_queue()

        channel.declare_exchange.assert_called_once_with(exchange_name, passive=True)
        channel.declare_queue.assert_called_once_with(exclusive=True)
        assert channel.declare_queue.return_value is result
        result.consume.assert_called_once_with(cb, no_ack=True)

    @pytest.mark.asyncio
    async def test_create_queue_setup_properties(self):
        channel = AsyncMock(spec_set=aio_pika.Channel)
        exchange_name = 'an_exchange'

        def cb(): pass

        consumer = RabbitConsumer(channel, exchange_name, cb)

        assert consumer.channel is channel

        await consumer.create_queue()

        assert channel.declare_exchange.return_value is consumer.exchange
        assert channel.declare_queue.return_value is consumer.queue


def fixture_factory_constructor_cast_properties():
    # String properties
    props = (
        'exchange_info_dsn',
        'depth_scraping_queue_dsn',
        'depth_scraping_queue_exchange',
    )

    for prop in props:
        yield prop, 1, '1'
        yield prop, 2.0, '2.0'
        yield prop, False, 'False'
        yield prop, ' test ', 'test'

    # Float properties
    props = (
        'exchange_info_get_entities_timeout',
        'process_ready_timeout',
    )

    for prop in props:
        yield prop, '-1', -1
        yield prop, '1', 1
        yield prop, '2.0', 2.0


def fixture_init_starts_transport_process():
    yield None
    yield True
    yield False


def fixture_test_remote_methods_setup_callback():
    exchange_info_dsn = 'test.local'
    exchange_info_get_entities_timeout = 1.0
    constructor_args1 = {
        'exchange_info_dsn': exchange_info_dsn,
        'exchange_info_get_entities_timeout': exchange_info_get_entities_timeout,
    }
    response1 = GetExchangeEntitiesRequest(
        exchange_info_dsn,
        exchange_info_get_entities_timeout
    )

    yield constructor_args1, 'get_exchange_entities', {}, response1

    depth_scraping_queue_dsn = 'test.local'
    depth_scraping_queue_exchange = 'test-exchange'
    constructor_args2 = {
        'depth_scraping_queue_dsn': depth_scraping_queue_dsn,
        'depth_scraping_queue_exchange': depth_scraping_queue_exchange,
    }
    method_args2 = {
        'consume_keys': [],
    }
    response2 = ConsumePriceDepthRequest(
        depth_scraping_queue_dsn,
        depth_scraping_queue_exchange,
        frozenset()
    )

    yield constructor_args2, 'consume_price_depth', method_args2, response2


@pytest.mark.usefixtures('unexpected_exceptions_handler')
class TestRealTransportFactory:
    @pytest.mark.parametrize(
        'prop, arg_value, expected_value',
        fixture_factory_constructor_cast_properties()
    )
    def test_constructor_cast_properties(self, prop: str, arg_value: Any, expected_value: Any):
        factory = self._get_factory_instance(**{prop: arg_value})

        assert getattr(factory, prop) == expected_value

    @pytest.mark.asyncio
    @pytest.mark.parametrize('loop_debug', fixture_init_starts_transport_process())
    async def test_init_starts_transport_process(self, mocker: MockFixture, loop_debug: bool):
        process_cls = mocker.patch(
            'galts_trade_api.transport.real.RealTransportProcess',
            autospec=True
        )
        process_cls.side_effect = self._factory_process_constructor_which_set_event(process_cls)

        factory = self._get_factory_instance()
        await factory.init(loop_debug=loop_debug)

        process_cls.assert_called_once_with(
            loop_debug=loop_debug,
            connection=ANY,
            ready_event=ANY
        )
        process_cls.instance.start.assert_called_once()

        cancel_other_async_tasks()

    @pytest.mark.asyncio
    async def test_init_exception_for_second_call(self, mocker: MockFixture):
        process_cls = mocker.patch(
            'galts_trade_api.transport.real.RealTransportProcess',
            autospec=True
        )
        process_cls.side_effect = self._factory_process_constructor_which_set_event(process_cls)

        factory = self._get_factory_instance()
        await factory.init()

        with pytest.raises(RuntimeError, match='should be created only once'):
            await factory.init()

        cancel_other_async_tasks()

    @pytest.mark.asyncio
    async def test_init_exception_for_long_transport_process_init(self, mocker: MockFixture):
        mocker.patch('galts_trade_api.transport.real.RealTransportProcess', autospec=True)

        factory = self._get_factory_instance(process_ready_timeout=0.1)

        with pytest.raises(RuntimeError, match='Failed to initialize'):
            await factory.init()

    @pytest.mark.asyncio
    async def test_init_starts_router_task(self, mocker: MockFixture):
        process_cls = mocker.patch(
            'galts_trade_api.transport.real.RealTransportProcess',
            autospec=True
        )
        process_cls.side_effect = self._factory_process_constructor_which_set_event(process_cls)
        router_cls = mocker.patch(
            'galts_trade_api.transport.real.PipeResponseRouter',
            autospec=True
        )
        router_instance = router_cls.return_value
        create_task = mocker.patch('asyncio.create_task', autospec=True)

        factory = self._get_factory_instance(process_ready_timeout=0.1)
        await factory.init()

        cancel_other_async_tasks()

        router_cls.assert_called_once()
        router_instance.start.assert_called_once()

        create_task.assert_called_once()
        create_task.return_value.add_done_callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_init_set_done_callback_for_router_task_which_shutdown_on_cancellation(
        self,
        mocker: MockFixture
    ):
        factory_shutdown = mocker.patch.object(RealTransportFactory, 'shutdown')
        process_cls = mocker.patch(
            'galts_trade_api.transport.real.RealTransportProcess',
            autospec=True
        )
        process_cls.side_effect = self._factory_process_constructor_which_set_event(process_cls)
        router_cls = mocker.patch(
            'galts_trade_api.transport.real.PipeResponseRouter',
            autospec=True
        )
        is_called = Event()

        async def start():
            is_called.set()
            await asyncio.sleep(1)

        router_cls.return_value.start.return_value = start()

        factory = self._get_factory_instance(process_ready_timeout=0.1)
        await factory.init()

        # Pass a loop iteration to execute the start and the done callback
        await asyncio.sleep(0.001)

        cancel_other_async_tasks()

        # Pass a loop iteration to execute the done callback
        await asyncio.sleep(0.001)

        factory_shutdown.assert_called_once()
        assert is_called.is_set()

    @pytest.mark.asyncio
    async def test_init_set_done_callback_for_router_task_which_shutdown_on_exception(
        self,
        mocker: MockFixture
    ):
        factory_shutdown = mocker.patch.object(RealTransportFactory, 'shutdown')
        process_cls = mocker.patch(
            'galts_trade_api.transport.real.RealTransportProcess',
            autospec=True
        )
        process_cls.side_effect = self._factory_process_constructor_which_set_event(process_cls)
        router_cls = mocker.patch(
            'galts_trade_api.transport.real.PipeResponseRouter',
            autospec=True
        )
        handler_is_called = Event()
        start_is_called = Event()

        expected_exception = RuntimeError('Halt router')

        def on_exception(_: asyncio.AbstractEventLoop, context: Dict[str, Any]) -> None:
            handler_is_called.set()
            assert context['exception'] is expected_exception

        asyncio.get_running_loop().set_exception_handler(on_exception)

        async def start():
            start_is_called.set()
            raise expected_exception

        router_cls.return_value.start.return_value = start()

        factory = self._get_factory_instance(process_ready_timeout=0.1)
        await factory.init()

        # Pass a loop iteration to execute the start and the done callback
        await asyncio.sleep(0.001)

        factory_shutdown.assert_called_once()
        assert start_is_called.is_set()
        assert handler_is_called.is_set()

    @pytest.mark.asyncio
    async def test_shutdown_calls_process(self, mocker: MockFixture):
        process_cls = mocker.patch(
            'galts_trade_api.transport.real.RealTransportProcess',
            autospec=True
        )
        process_cls.side_effect = self._factory_process_constructor_which_set_event(process_cls)

        factory = self._get_factory_instance()
        await factory.init()
        factory.shutdown()

        process_cls.instance.terminate.assert_called_once()

        cancel_other_async_tasks()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'factory_args, factory_method_name, factory_method_args, expected_request',
        fixture_test_remote_methods_setup_callback()
    )
    async def test_remote_methods_setup_callback(
        self,
        mocker: MockFixture,
        factory_args: Mapping,
        factory_method_name: str,
        factory_method_args: Mapping,
        expected_request: PipeRequest
    ):
        expected_response = 'test response'

        pipe_cls = mocker.patch('galts_trade_api.transport.real.Pipe', autospec=True)
        parent_connection_mock = Mock(spec_set=Connection, **{'poll.return_value': False})
        child_connection_mock = Mock(spec_set=Connection)
        pipe_cls.return_value = (parent_connection_mock, child_connection_mock)
        process_cls = mocker.patch(
            'galts_trade_api.transport.real.RealTransportProcess',
            autospec=True
        )
        process_cls.side_effect = self._factory_process_constructor_which_set_event(process_cls)
        is_called = Event()

        async def cb(data):
            is_called.set()
            assert data == expected_response

        factory = self._get_factory_instance(**factory_args)
        await factory.init()
        consumer = await getattr(factory, factory_method_name)(cb, **factory_method_args)
        await consumer.send(expected_response)

        parent_connection_mock.send.assert_called_once_with(expected_request)
        assert is_called.is_set()

        cancel_other_async_tasks()

    @classmethod
    def _get_factory_instance(
        cls,
        exchange_info_dsn: str = 'test.local',
        depth_scraping_queue_dsn: str = 'test.local',
        depth_scraping_queue_exchange: str = 'test-exchange',
        exchange_info_get_entities_timeout: float = 5.0,
        process_ready_timeout: float = 2.0,
    ) -> RealTransportFactory:
        return RealTransportFactory(
            exchange_info_dsn=exchange_info_dsn,
            depth_scraping_queue_dsn=depth_scraping_queue_dsn,
            depth_scraping_queue_exchange=depth_scraping_queue_exchange,
            exchange_info_get_entities_timeout=exchange_info_get_entities_timeout,
            process_ready_timeout=process_ready_timeout,
        )

    @classmethod
    def _factory_process_constructor_which_set_event(
        cls,
        process_class: Mock
    ) -> Callable[..., Mock]:
        def process_constructor(ready_event, **kwargs) -> Mock:
            ready_event.set()
            instance = Mock()
            process_class.attach_mock(instance, 'instance')

            return instance

        return process_constructor


def fixture_process_constructor_cast_properties():
    # Float properties
    props = (
        'poll_delay',
    )

    for prop in props:
        yield prop, '-1', -1
        yield prop, '1', 1
        yield prop, '2.0', 2.0


def fixture_run_calls_async_helper():
    yield None
    yield True
    yield False


def fixture_consume_price_depth():
    yield frozenset(), ['#']
    yield frozenset([DepthConsumeKey('test1'), DepthConsumeKey('test2')]), \
        ['test1.*.*', 'test2.*.*']


class TestRealTransportProcess:
    @pytest.mark.parametrize(
        'prop, arg_value, expected_value',
        fixture_process_constructor_cast_properties()
    )
    def test_constructor_cast_properties(self, prop: str, arg_value: Any, expected_value: Any):
        process = RealTransportProcess(
            ready_event=Event(),
            connection=Mock(spec_set=Connection),
            poll_delay=arg_value,
        )

        assert getattr(process, prop) == expected_value

    @pytest.mark.parametrize('loop_debug', fixture_run_calls_async_helper())
    def test_run_calls_async_helper(self, mocker: MockFixture, loop_debug: bool):
        helper_mock = mocker.patch('galts_trade_api.transport.real.run_program_forever')

        process = RealTransportProcess(
            loop_debug=loop_debug,
            ready_event=Event(),
            connection=Mock(spec_set=Connection)
        )
        process.run()

        helper_mock.assert_called_once_with(process.main, loop_debug=loop_debug)

    @pytest.mark.asyncio
    async def test_add_handler_add_ability_to_call_handler(self):
        env_mock = Mock(spec_set=AsyncProgramEnv)
        parent_connection, child_connection = Pipe()
        process = RealTransportProcess(ready_event=Event(), connection=child_connection)
        handler_was_called = Event()
        request_type = TestRequest

        async def handler(r: PipeRequest):
            handler_was_called.set()
            assert isinstance(r, request_type)

        process.add_handler(TestRequest, handler)

        process_task = asyncio.create_task(process.main(env_mock))
        parent_connection.send(request_type())
        await asyncio.sleep(0.001)

        assert handler_was_called.is_set()

        assert not process_task.cancelled()
        process_task.cancel()

    @pytest.mark.asyncio
    async def test_add_handler_cannot_override_handler(self):
        process = RealTransportProcess(ready_event=Event(), connection=Mock(spec_set=Connection))

        async def handler(_): pass

        process.add_handler(TestRequest, handler)

        with pytest.raises(ValueError, match='already registered'):
            process.add_handler(TestRequest, handler)

    @pytest.mark.asyncio
    async def test_main_set_event(self):
        event = Event()
        connection_mock = Mock(spec_set=Connection, **{'poll.return_value': False})
        process = RealTransportProcess(ready_event=event, connection=connection_mock)
        env_mock = Mock(spec_set=AsyncProgramEnv)

        process_task = asyncio.create_task(process.main(env_mock))
        await asyncio.sleep(0.001)

        assert not process_task.cancelled()
        assert event.is_set()

        process_task.cancel()

    @pytest.mark.asyncio
    async def test_main_set_exception_handler(self, mocker: MockFixture):
        shutdown_mock = mocker.patch('galts_trade_api.asyncio_helper.shutdown')
        env = AsyncProgramEnv()
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(env.exception_handler)
        parent_connection, child_connection = Pipe()
        process = RealTransportProcess(ready_event=Event(), connection=child_connection)
        handler_was_called = Event()

        async def handler(_):
            handler_was_called.set()
            raise RuntimeError('Test exception from a handler')

        process.add_handler(TestRequest, handler)

        assert not env.exception_handler_patch

        process_task = asyncio.create_task(process.main(env))
        parent_connection.send(TestRequest())
        await asyncio.sleep(0.001)

        assert env.exception_handler_patch
        assert handler_was_called.is_set()

        assert parent_connection.poll(0.001)
        response = parent_connection.recv()
        assert len(response) == 1
        assert isinstance(response[0], TransportFactoryException)

        shutdown_mock.assert_called_once()
        assert not process_task.cancelled()
        process_task.cancel()

    @pytest.mark.asyncio
    async def test_main_exception_for_unknown_handler(self):
        env_mock = Mock(spec_set=AsyncProgramEnv)
        parent_connection, child_connection = Pipe()
        process = RealTransportProcess(ready_event=Event(), connection=child_connection)

        process_task = asyncio.create_task(process.main(env_mock))
        parent_connection.send(TestRequest())
        await asyncio.sleep(0.001)

        assert not process_task.cancelled()

        with pytest.raises(ValueError, match='No handler'):
            process_task.result()

    @pytest.mark.asyncio
    async def test_get_exchange_entities_calls_exchange_info(self, mocker: MockFixture):
        expected_response = {'asset': []}
        client_mock = mocker.patch(
            'galts_trade_api.transport.real.ExchangeInfoClient',
            autospec=True
        )
        client_instance_mock = client_mock.factory.return_value
        client_instance_mock.get_entities.return_value = expected_response
        env_mock = Mock(spec_set=AsyncProgramEnv)
        parent_connection, child_connection = Pipe()
        process = RealTransportProcess(ready_event=Event(), connection=child_connection)

        process_task = asyncio.create_task(process.main(env_mock))

        dsn = 'test.local'
        timeout = 1.0
        request = GetExchangeEntitiesRequest(dsn, timeout)
        parent_connection.send(request)
        await asyncio.sleep(0.001)

        client_mock.factory.assert_called_once_with(dsn, timeout_get_entities=timeout)
        client_instance_mock.get_entities.assert_called_once()
        client_instance_mock.destroy.assert_called_once()

        assert parent_connection.poll(0.001)
        response = parent_connection.recv()
        assert len(response) == 2
        assert response[0] == request
        assert response[1] == expected_response

        assert not process_task.cancelled()
        process_task.cancel()

    @pytest.mark.asyncio
    @pytest.mark.parametrize('keys, expected_bind_exchanges', fixture_consume_price_depth())
    async def test_consume_price_depth(
        self,
        mocker: MockFixture,
        keys: FrozenSet,
        expected_bind_exchanges: Sequence
    ):
        connection_mock = mocker.patch(
            'galts_trade_api.transport.real.RabbitConnection',
            autospec=True
        )
        connection_instance_mock = connection_mock.return_value
        connection_instance_mock.create_channel = AsyncMock()
        channel_mock = connection_instance_mock.create_channel.return_value

        consumer_mock = mocker.patch('galts_trade_api.transport.real.RabbitConsumer', autospec=True)
        consumer_instance_mock = consumer_mock.return_value
        consumer_instance_mock.create_queue = AsyncMock()
        queue_mock = consumer_instance_mock.create_queue.return_value

        env_mock = Mock(spec_set=AsyncProgramEnv)
        parent_connection, child_connection = Pipe()
        process = RealTransportProcess(ready_event=Event(), connection=child_connection)

        process_task = asyncio.create_task(process.main(env_mock))

        dsn = 'test.local'
        exchange = 'test-exchange'
        request = ConsumePriceDepthRequest(dsn, exchange, keys)
        parent_connection.send(request)
        await asyncio.sleep(0.001)

        connection_mock.assert_called_once_with(dsn)
        connection_instance_mock.create_channel.assert_called_once_with(100)
        consumer_mock.assert_called_once_with(channel_mock, exchange, ANY)
        consumer_instance_mock.create_queue.assert_called_once()
        for expected_bind_exchange in expected_bind_exchanges:
            queue_mock.bind.assert_any_call(consumer_instance_mock.exchange, expected_bind_exchange)

        # This type of request don't answered synchronously
        assert not parent_connection.poll(0.001)

        assert not process_task.cancelled()
        process_task.cancel()


class TestRequest(PipeRequest):
    pass
