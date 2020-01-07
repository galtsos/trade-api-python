from unittest.mock import patch

import pytest

from galts_trade_api.transport.real import RabbitConnection
from ..utils import AsyncMock


class TestRabbitConnection:
    def test_constructor_dont_init_connection_object(self):
        conn = RabbitConnection('')

        assert conn.connection is None

    @pytest.mark.asyncio
    async def test_constructor_trim_dsn(self):
        dsn = ' \t test.local  '
        expected_dsn = dsn.strip()
        assert dsn != expected_dsn

        conn = RabbitConnection(dsn)

        with patch('aio_pika.connect_robust', new_callable=AsyncMock) as connect_robust:
            await conn.create_channel()

        connect_robust.assert_called_once_with(expected_dsn)

    @pytest.mark.asyncio
    async def test_create_channel_reuse_one_connection(self):
        conn = RabbitConnection('test.local')

        with patch('aio_pika.connect_robust', new_callable=AsyncMock):
            await conn.create_channel()
            first_execution_connection = conn.connection
            await conn.create_channel()

        assert conn.connection is first_execution_connection

    @pytest.mark.asyncio
    async def test_create_channel_use_delivered_dsn(self):
        dsn = 'test.local'
        conn = RabbitConnection(dsn)
        prefetch_count = 50

        with patch('aio_pika.connect_robust', new_callable=AsyncMock) as connect_robust:
            result = await conn.create_channel(prefetch_count=prefetch_count)

        connect_robust.assert_called_once_with(dsn)
        result.set_qos.assert_called_once_with(prefetch_count=prefetch_count)
