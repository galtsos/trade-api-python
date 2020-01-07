from unittest.mock import Mock, patch

import aio_pika
import pytest

from galts_trade_api.transport.real import RabbitConnection, RabbitConsumer
from ..utils import AsyncMock


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
    async def test_constructor_trim_dsn(self):
        dsn = ' \t test4.local  '
        expected_dsn = dsn.strip()
        assert dsn != expected_dsn

        conn = RabbitConnection(dsn)

        with patch('aio_pika.connect_robust', new_callable=AsyncMock) as connect_robust:
            await conn.create_channel()

        connect_robust.assert_called_once_with(expected_dsn)

    @pytest.mark.asyncio
    async def test_create_channel_reuse_one_connection(self):
        conn = RabbitConnection('test5.local')

        with patch('aio_pika.connect_robust', new_callable=AsyncMock):
            await conn.create_channel()
            first_execution_connection = conn.connection
            await conn.create_channel()

        assert conn.connection is first_execution_connection

    @pytest.mark.asyncio
    async def test_create_channel_use_delivered_dsn(self):
        dsn = 'test6.local'
        conn = RabbitConnection(dsn)
        prefetch_count = 50

        with patch('aio_pika.connect_robust', new_callable=AsyncMock) as connect_robust:
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
