import asyncio
from typing import Awaitable, Callable, List, Mapping, Optional, Sequence
from unittest.mock import ANY, Mock, call, patch

import pytest

from galts_trade_api.terminal import Terminal
from galts_trade_api.transport import DepthConsumeKey, MessageConsumerCollection, TransportFactory
from .utils import AsyncMock


def fixture_init_transport_calls_factory():
    yield None
    yield True
    yield False


def fixture_init_exchange_entities_exception_for_missed_key():
    yield {}, 'exchanges'
    yield {'exchanges': {}}, 'markets'
    yield {'exchanges': {}, 'markets': {}}, 'symbols'
    yield {'exchanges': {}, 'markets': {}, 'symbols': {}}, 'assets'


def fixture_init_exchange_entities_exception_on_data_inconsistency():
    empty_data = {'exchanges': {}, 'markets': {}, 'symbols': {}, 'assets': {}}
    stub_record = {'tag': 'foo', 'delete_time': None}

    yield {**empty_data, 'assets': {1: stub_record, 2: stub_record}}, \
        'Assets with duplicates in tags found: foo'

    yield {**empty_data, 'exchanges': {1: stub_record, 2: stub_record}}, \
        'Exchanges with duplicates in tags found: foo'

    yield {
        **empty_data,
        'exchanges': {
            1: {
                'id': 1,
                'tag': 'exchange',
                'name': 'Exchange',
                'create_time': None,
                'delete_time': None,
                'disable_time': None,
            },
        },
        'markets': {
            1: {
                'id': 1,
                'exchange_id': 2,
                'delete_time': None,
            },
        },
    }, 'No exchange with id 2 has been found for market with id 1'


def fixture_init_exchange_entities_ignore_deleted_entities():
    empty_data = {'exchanges': {}, 'markets': {}, 'symbols': {}, 'assets': {}}

    asset = {
        'id': 1,
        'tag': 'asset-a',
        'name': 'Asset A',
        'precision': 2,
        'create_time': None,
        'delete_time': None,
    }

    yield 'Asset', {
        **empty_data,
        'assets': {
            1: asset,
            2: {**asset, **{'id': 2, 'delete_time': True}},
        },
    }, call(**asset)

    symbol = {
        'id': 1,
        'base_asset_id': 3,
        'quote_asset_id': 4,
        'create_time': None,
        'delete_time': None,
    }

    yield 'Symbol', {
        **empty_data,
        'symbols': {
            1: symbol,
            2: {**symbol, **{'id': 2, 'delete_time': True}},
        },
    }, call(**symbol)

    exchange = {
        'id': 1,
        'tag': 'exchange-a',
        'name': 'Exchange A',
        'create_time': None,
        'delete_time': None,
        'disable_time': None,
    }

    yield 'Exchange', {
        **empty_data,
        'exchanges': {
            1: exchange,
            2: {**exchange, **{'id': 2, 'delete_time': True}},
        },
    }, call(**exchange)

    market = {
        'id': 1,
        'custom_tag': 'tag-a',
        'exchange_id': 5,
        'symbol_id': 6,
        'trade_endpoint': 'test.local',
        'create_time': None,
        'delete_time': None,
    }

    yield 'Market', {
        **empty_data,
        'exchanges': {
            5: {
                'id': 5,
                'tag': 'exchange-a',
                'name': 'Exchange A',
                'create_time': None,
                'delete_time': None,
                'disable_time': None,
            },
        },
        'markets': {
            1: market,
            2: {**market, **{'id': 2, 'delete_time': True}},
        },
    }, call(**market)


class TestTerminal:
    def test_transport_factory_property(self):
        factory1 = Mock(spec_set=TransportFactory)
        factory2 = Mock(spec_set=TransportFactory)

        terminal = Terminal(factory1)
        assert terminal.transport_factory is factory1
        terminal.transport_factory = factory2
        assert terminal.transport_factory is factory2

    @pytest.mark.asyncio
    @pytest.mark.parametrize('loop_debug', fixture_init_transport_calls_factory())
    async def test_init_transport_calls_factory(self, loop_debug):
        factory = AsyncMock(spec_set=TransportFactory)
        terminal = Terminal(factory)
        await terminal.init_transport(loop_debug)

        factory.init.assert_called_once_with(loop_debug)

    def test_shutdown_transport_calls_factory(self):
        factory = Mock(spec_set=TransportFactory)
        terminal = Terminal(factory)
        terminal.shutdown_transport()

        factory.shutdown.assert_called_once_with()

    @pytest.mark.asyncio
    @pytest.mark.skip('SUT have no realization')
    async def test_auth_user(self):
        pass

    def test_is_exchange_entities_inited_after_instantiation(self):
        factory = Mock(spec_set=TransportFactory)
        terminal = Terminal(factory)

        assert not terminal.is_exchange_entities_inited()

    @pytest.mark.asyncio
    async def test_wait_exchange_entities_inited_exception_after_timeout(self):
        factory = Mock(spec_set=TransportFactory)
        terminal = Terminal(factory)

        with pytest.raises(asyncio.TimeoutError):
            await terminal.wait_exchange_entities_inited(0.001)

    @pytest.mark.asyncio
    async def test_init_exchange_entities_calls_factory(self):
        factory = AsyncMock(spec_set=TransportFactory)
        terminal = Terminal(factory)
        await terminal.init_exchange_entities()

        factory.get_exchange_entities.assert_called_once_with(ANY)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'data, expected_key_name',
        fixture_init_exchange_entities_exception_for_missed_key()
    )
    async def test_init_exchange_entities_exception_for_missed_key(
        self,
        data: Mapping,
        expected_key_name: str
    ):
        factory_fake = FakeTransportFactory()
        factory_fake.init_exchange_entities_data = data

        terminal = Terminal(factory_fake)
        with pytest.raises(KeyError, match=f'Key "{expected_key_name}" is required'):
            await terminal.init_exchange_entities()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'data, expected_message',
        fixture_init_exchange_entities_exception_on_data_inconsistency()
    )
    async def test_init_exchange_entities_exception_on_data_inconsistency(
        self,
        data: Mapping,
        expected_message: str
    ):
        factory_fake = FakeTransportFactory()
        factory_fake.init_exchange_entities_data = data

        terminal = Terminal(factory_fake)
        with pytest.raises(ValueError, match=expected_message):
            await terminal.init_exchange_entities()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'entity_class_name, data, expected_call',
        fixture_init_exchange_entities_ignore_deleted_entities()
    )
    async def test_init_exchange_entities_ignore_deleted_entities(
        self,
        mocker,
        entity_class_name: str,
        data: Mapping,
        expected_call: Sequence,
    ):
        entity_class_mock = mocker.patch(
            f'galts_trade_api.terminal.{entity_class_name}',
            autospec=True
        )

        factory_fake = FakeTransportFactory()
        factory_fake.init_exchange_entities_data = data

        terminal = Terminal(factory_fake)
        await terminal.init_exchange_entities()

        assert entity_class_mock.call_args == expected_call

    @pytest.mark.asyncio
    async def test_init_exchange_entities_set_event(self):
        data = {'exchanges': {}, 'markets': {}, 'symbols': {}, 'assets': {}}
        factory_fake = FakeTransportFactory()
        factory_fake.init_exchange_entities_data = data

        terminal = Terminal(factory_fake)
        await terminal.init_exchange_entities()

        assert terminal.is_exchange_entities_inited()
        await terminal.wait_exchange_entities_inited(0.001)


class FakeTransportFactory(TransportFactory):
    def __init__(self):
        self.init_exchange_entities_data: Optional[Mapping] = None

    async def get_exchange_entities(
        self,
        on_response: Callable[..., Awaitable]
    ) -> MessageConsumerCollection:
        result = MessageConsumerCollection()
        result.add_consumer(on_response)
        await result.send(self.init_exchange_entities_data)

        return result

    async def consume_price_depth(
        self,
        on_response: Callable[..., Awaitable],
        consume_keys: Optional[List[DepthConsumeKey]] = None
    ) -> MessageConsumerCollection:
        pass
