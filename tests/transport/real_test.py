import asyncio
from multiprocessing.connection import Connection
from typing import Any, Callable, Dict
from unittest.mock import ANY, Mock

import aio_pika
import pytest

from galts_trade_api.transport.real import ConsumePriceDepthRequest, \
    GetExchangeEntitiesRequest, RabbitConnection, RabbitConsumer, RealTransportFactory
from ..utils import AsyncMock, cancel_other_tasks


@pytest.fixture
async def unexpected_exceptions_handler():
    def on_exception(_: asyncio.AbstractEventLoop, context: Dict[str, Any]) -> None:
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
    async def test_constructor_trim_dsn(self, mocker):
        dsn = ' \t test4.local  '
        expected_dsn = dsn.strip()
        assert dsn != expected_dsn

        connect_robust = mocker.patch('aio_pika.connect_robust', new_callable=AsyncMock)

        conn = RabbitConnection(dsn)
        await conn.create_channel()

        connect_robust.assert_called_once_with(expected_dsn)

    @pytest.mark.asyncio
    async def test_create_channel_reuse_one_connection(self, mocker):
        mocker.patch('aio_pika.connect_robust', new_callable=AsyncMock)

        conn = RabbitConnection('test5.local')
        await conn.create_channel()
        first_execution_connection = conn.connection
        await conn.create_channel()

        assert conn.connection is first_execution_connection

    @pytest.mark.asyncio
    async def test_create_channel_use_delivered_dsn(self, mocker):
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


def fixture_constructor_cast_properties():
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
    @pytest.mark.parametrize('prop, arg_value, expected_value',
        fixture_constructor_cast_properties())
    def test_constructor_cast_properties(self, prop, arg_value, expected_value):
        factory = self._get_factory_instance(**{prop: arg_value})

        assert getattr(factory, prop) == expected_value

    @pytest.mark.asyncio
    @pytest.mark.parametrize('loop_debug', fixture_init_starts_transport_process())
    async def test_init_starts_transport_process(self, mocker, loop_debug):
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

        cancel_other_tasks()

    @pytest.mark.asyncio
    async def test_init_exception_for_second_call(self, mocker):
        process_cls = mocker.patch(
            'galts_trade_api.transport.real.RealTransportProcess',
            autospec=True
        )
        process_cls.side_effect = self._factory_process_constructor_which_set_event(process_cls)

        factory = self._get_factory_instance()

        with pytest.raises(RuntimeError, match='should be created only once'):
            await factory.init()
            await factory.init()

        cancel_other_tasks()

    @pytest.mark.asyncio
    async def test_init_exception_for_long_transport_process_init(self, mocker):
        mocker.patch('galts_trade_api.transport.real.RealTransportProcess', autospec=True)

        factory = self._get_factory_instance(process_ready_timeout=0.1)

        with pytest.raises(RuntimeError, match='Failed to initialize'):
            await factory.init()

    @pytest.mark.asyncio
    async def test_init_starts_router_task(self, mocker):
        process_cls = mocker.patch(
            'galts_trade_api.transport.real.RealTransportProcess',
            autospec=True
        )
        process_cls.side_effect = self._factory_process_constructor_which_set_event(process_cls)
        router_cls = mocker.patch(
            'galts_trade_api.transport.real.PipeResponseRouter',
            autospec=True
        )
        create_task = mocker.patch('asyncio.create_task', autospec=True)

        factory = self._get_factory_instance(process_ready_timeout=0.1)
        await factory.init()

        cancel_other_tasks()

        router_cls.assert_called_once()
        router_cls.return_value.start.assert_called_once()

        create_task.assert_called_once_with(router_cls.return_value.start())
        create_task.return_value.add_done_callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_init_set_done_callback_for_router_task_which_shutdown_on_cancellation(
        self,
        mocker
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

        async def start(): await asyncio.sleep(1)

        router_cls.return_value.start.return_value = start()

        factory = self._get_factory_instance(process_ready_timeout=0.1)
        await factory.init()

        cancel_other_tasks()

        # Pass a loop iteration to execute the done callback
        await asyncio.sleep(0.001)

        factory_shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_init_set_done_callback_for_router_task_which_shutdown_on_exception(self, mocker):
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

        expected_exception = RuntimeError('Halt router')

        def on_exception(_: asyncio.AbstractEventLoop, context: Dict[str, Any]) -> None:
            assert context['exception'] is expected_exception

        asyncio.get_running_loop().set_exception_handler(on_exception)

        async def start(): raise expected_exception

        router_cls.return_value.start.return_value = start()

        factory = self._get_factory_instance(process_ready_timeout=0.1)
        await factory.init()

        # Pass a loop iteration to execute the done callback
        await asyncio.sleep(0.001)

        factory_shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_calls_process(self, mocker):
        process_cls = mocker.patch(
            'galts_trade_api.transport.real.RealTransportProcess',
            autospec=True
        )
        process_cls.side_effect = self._factory_process_constructor_which_set_event(process_cls)

        factory = self._get_factory_instance()
        await factory.init()
        factory.shutdown()

        process_cls.instance.terminate.assert_called_once()

        cancel_other_tasks()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'factory_args, factory_method_name, factory_method_args, expected_request',
        fixture_test_remote_methods_setup_callback()
    )
    async def test_remote_methods_setup_callback(
        self,
        mocker,
        factory_args,
        factory_method_name,
        factory_method_args,
        expected_request
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

        async def cb(data):
            assert data == expected_response

        factory = self._get_factory_instance(**factory_args)
        await factory.init()
        consumer = await getattr(factory, factory_method_name)(cb, **factory_method_args)
        await consumer.send(expected_response)

        parent_connection_mock.send.assert_called_once_with(expected_request)

        cancel_other_tasks()

    @classmethod
    def _get_factory_instance(
        cls,
        exchange_info_dsn: str = 'test.local',
        depth_scraping_queue_dsn: str = 'test.local',
        depth_scraping_queue_exchange: str = 'test-exchange',
        exchange_info_get_entities_timeout: float = 5.0,
        process_ready_timeout: float = 2.0,
    ):
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
