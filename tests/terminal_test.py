import asyncio
from asyncio import Event
from datetime import datetime
from decimal import Decimal
from typing import Any, Awaitable, Callable, List, Mapping, Optional, Sequence, Tuple
from unittest.mock import ANY, Mock

import pytest

from galts_trade_api.asset import DealSide
from galts_trade_api.terminal import MarketsDepthsBuffer, Terminal
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
        'assets': {
            1: {
                'id': 1,
                'tag': 'asset-a',
                'name': 'Asset A',
                'precision': 2,
                'create_time': None,
                'delete_time': None,
            },
        },
        'symbols': {
            1: {
                'id': 1,
                'base_asset_id': 2,
                'quote_asset_id': 3,
                'delete_time': None,
            },
        },
    }, 'No base asset with id 2 has been found for symbol with id 1'

    yield {
        **empty_data,
        'assets': {
            1: {
                'id': 1,
                'tag': 'asset-a',
                'name': 'Asset A',
                'precision': 2,
                'create_time': None,
                'delete_time': None,
            },
            2: {
                'id': 2,
                'tag': 'asset-b',
                'name': 'Asset B',
                'precision': 2,
                'create_time': None,
                'delete_time': None,
            },
        },
        'symbols': {
            1: {
                'id': 1,
                'base_asset_id': 2,
                'quote_asset_id': 3,
                'delete_time': None,
            },
        },
    }, 'No quote asset with id 3 has been found for symbol with id 1'

    yield {
        **empty_data,
        'assets': {
            1: {
                'id': 1,
                'tag': 'ASS1',
                'name': 'Asset A',
                'precision': 2,
                'create_time': None,
                'delete_time': None,
            },
            2: {
                'id': 2,
                'tag': 'ASS2',
                'name': 'Asset B',
                'precision': 2,
                'create_time': None,
                'delete_time': None,
            },
        },
        'symbols': {
            1: {
                'id': 1,
                'base_asset_id': 1,
                'quote_asset_id': 2,
                'create_time': None,
                'delete_time': None,
            },
            2: {
                'id': 1,
                'base_asset_id': 1,
                'quote_asset_id': 2,
                'create_time': None,
                'delete_time': None,
            },
        },
    }, 'Symbols with duplicates in tags found: ASS1ASS2'

    yield {
        **empty_data,
        'exchanges': {
            1: {
                'id': 1,
                'tag': 'exchange-a',
                'name': 'Exchange A',
                'create_time': None,
                'delete_time': None,
                'disable_time': None,
            },
        },
        'markets': {
            1: {
                'id': 1,
                'exchange_id': 2,
                'symbol_id': 6,
                'delete_time': None,
            },
        },
    }, 'No exchange with id 2 has been found for market with id 1'

    yield {
        **empty_data,
        'exchanges': {
            1: {
                'id': 1,
                'tag': 'exchange-a',
                'name': 'Exchange A',
                'create_time': None,
                'delete_time': None,
                'disable_time': None,
            },
            2: {
                'id': 2,
                'tag': 'exchange-b',
                'name': 'Exchange B',
                'create_time': None,
                'delete_time': None,
                'disable_time': None,
            },
        },
        'markets': {
            1: {
                'id': 1,
                'exchange_id': 2,
                'symbol_id': 6,
                'delete_time': None,
            },
        },
    }, 'No symbol with id 6 has been found for market with id 1'


def fixture_init_exchange_entities_ignore_deleted_entities():
    asset = {
        'id': 1,
        'tag': 'asset-a',
        'name': 'Asset A',
        'precision': 2,
        'create_time': None,
        'delete_time': None,
    }
    symbol = {
        'id': 1,
        'base_asset_id': 1,
        'quote_asset_id': 2,
        'create_time': None,
        'delete_time': None,
    }
    exchange = {
        'id': 1,
        'tag': 'exchange-a',
        'name': 'Exchange A',
        'create_time': None,
        'delete_time': None,
        'disable_time': None,
    }
    market = {
        'id': 1,
        'custom_tag': 'tag-a',
        'exchange_id': 1,
        'symbol_id': 1,
        'trade_endpoint': 'test.local',
        'create_time': None,
        'delete_time': None,
    }

    yield {
        'assets': {
            1: asset,
            2: {**asset, **{'id': 2, 'tag': 'asset-b', }},
            5: {**asset, **{'id': 5, 'delete_time': True}},
        },
        'symbols': {
            1: symbol,
            6: {**symbol, **{'id': 6, 'delete_time': datetime.utcnow()}},
        },
        'exchanges': {
            1: exchange,
            3: {**exchange, **{'id': 3, 'tag': 'exchange-b', 'delete_time': datetime.utcnow()}},
        },
        'markets': {
            1: market,
            2: {**market, **{'id': 2, 'custom_tag': 'tag-b', 'delete_time': True}},
        },
    }, [1, 2], [1], [1], [1]


class TestTerminal:
    def test_properties(self):
        factory1 = Mock(spec_set=TransportFactory)
        factory2 = Mock(spec_set=TransportFactory)
        depths = MarketsDepthsBuffer()

        terminal = Terminal(factory1, depths)
        assert terminal.transport_factory is factory1
        assert terminal.depths is depths
        terminal.transport_factory = factory2
        assert terminal.transport_factory is factory2

    @pytest.mark.asyncio
    @pytest.mark.parametrize('loop_debug', fixture_init_transport_calls_factory())
    async def test_init_transport_calls_factory(self, loop_debug: bool):
        factory = AsyncMock(spec_set=TransportFactory)
        terminal = factory_terminal(factory)
        await terminal.init_transport(loop_debug)

        factory.init.assert_called_once_with(loop_debug)

    def test_shutdown_transport_calls_factory(self):
        factory = Mock(spec_set=TransportFactory)
        terminal = factory_terminal(factory)
        terminal.shutdown_transport()

        factory.shutdown.assert_called_once_with()

    @pytest.mark.asyncio
    @pytest.mark.skip('SUT have no realization')
    async def test_auth_user(self):
        pass

    def test_is_exchange_entities_not_inited_after_instantiation(self):
        terminal = factory_terminal()

        assert not terminal.is_exchange_entities_inited()

    @pytest.mark.asyncio
    async def test_wait_exchange_entities_inited_exception_after_timeout(self):
        terminal = factory_terminal()

        with pytest.raises(asyncio.TimeoutError):
            await terminal.wait_exchange_entities_inited(0.001)

    @pytest.mark.asyncio
    async def test_init_exchange_entities_calls_factory(self):
        factory = AsyncMock(spec_set=TransportFactory)
        terminal = factory_terminal(factory)
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
        factory_fake.get_exchange_entities_data = data
        terminal = factory_terminal(factory_fake)

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
        factory_fake.get_exchange_entities_data = data
        terminal = factory_terminal(factory_fake)

        with pytest.raises(ValueError, match=expected_message):
            await terminal.init_exchange_entities()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'entities_data, expected_assets_ids, expected_symbols_ids, expected_exchanges_ids, '
        'expected_markets_ids',
        fixture_init_exchange_entities_ignore_deleted_entities()
    )
    async def test_init_exchange_entities_ignore_deleted_entities(
        self,
        entities_data: Mapping,
        expected_assets_ids: Sequence[int],
        expected_symbols_ids: Sequence[int],
        expected_exchanges_ids: Sequence[int],
        expected_markets_ids: Sequence[int]
    ):
        factory_fake = FakeTransportFactory()
        factory_fake.get_exchange_entities_data = entities_data

        terminal = factory_terminal(factory_fake)
        await terminal.init_exchange_entities()

        assert list(terminal.assets_by_id.keys()) == expected_assets_ids
        assert list(terminal.symbols_by_id.keys()) == expected_symbols_ids
        assert list(terminal.exchanges_by_id.keys()) == expected_exchanges_ids
        if len(expected_markets_ids):
            assert list(terminal.exchanges_by_id[1].markets_by_id.keys()) == expected_markets_ids

    @pytest.mark.asyncio
    async def test_init_exchange_entities_set_event(self):
        data = {'exchanges': {}, 'markets': {}, 'symbols': {}, 'assets': {}}
        factory_fake = FakeTransportFactory()
        factory_fake.get_exchange_entities_data = data

        terminal = factory_terminal(factory_fake)
        await terminal.init_exchange_entities()

        assert terminal.is_exchange_entities_inited()
        await terminal.wait_exchange_entities_inited(0.001)

    @pytest.mark.asyncio
    async def test_getters_return_inited_data(self):
        factory_fake = FakeTransportFactory()
        terminal = factory_terminal(factory_fake)

        getters = [
            terminal.assets_by_id,
            terminal.assets_by_tag,
            terminal.symbols_by_id,
            terminal.symbols_by_tag,
            terminal.exchanges_by_id,
            terminal.exchanges_by_tag,
        ]

        for getter in getters:
            assert len(getter) == 0, 'Getter should be empty before init_exchange_entities() call'

            with pytest.raises(KeyError):
                _ = getter[1]

        data = {
            'assets': {
                1: {
                    'id': 1,
                    'tag': 'AS1',
                    'name': 'Asset A',
                    'precision': 2,
                    'create_time': None,
                    'delete_time': None,
                },
                2: {
                    'id': 2,
                    'tag': 'AS2',
                    'name': 'Asset B',
                    'precision': 2,
                    'create_time': None,
                    'delete_time': None,
                },
            },
            'symbols': {
                1: {
                    'id': 1,
                    'base_asset_id': 1,
                    'quote_asset_id': 2,
                    'create_time': None,
                    'delete_time': None,
                },
            },
            'exchanges': {
                1: {
                    'id': 1,
                    'tag': 'exchange-a',
                    'name': 'Exchange',
                    'create_time': None,
                    'delete_time': None,
                    'disable_time': None,
                },
            },
            'markets': {
                1: {
                    'id': 1,
                    'custom_tag': 'market-a',
                    'exchange_id': 1,
                    'symbol_id': 1,
                    'trade_endpoint': 'test.local',
                    'create_time': None,
                    'delete_time': None,
                },
            },
        }
        factory_fake.get_exchange_entities_data = data

        await terminal.init_exchange_entities()

        assert terminal.assets_by_id[1].id == 1
        assert terminal.assets_by_tag['AS1'] is terminal.assets_by_id[1]
        assert terminal.assets_by_id[2].id == 2
        assert terminal.assets_by_tag['AS2'] is terminal.assets_by_id[2]

        assert terminal.symbols_by_id[1].id == 1
        assert terminal.symbols_by_tag['AS1AS2'] is terminal.symbols_by_id[1]

        exchange = terminal.exchanges_by_id[1]
        assert exchange.id == 1
        assert terminal.exchanges_by_tag['exchange-a'] is exchange

        assert exchange.markets_by_id[1].id == 1
        assert exchange.markets_by_tag['market-a'] is exchange.markets_by_id[1]

    @pytest.mark.asyncio
    async def test_subscribe_to_prices_calls_factory(self):
        keys = []

        factory = AsyncMock(spec_set=TransportFactory)
        terminal = factory_terminal(factory)
        await terminal.subscribe_to_prices(lambda: None, keys)

        factory.consume_price_depth.assert_called_once_with(ANY, keys)

    @pytest.mark.asyncio
    async def test_subscribe_to_prices(self):
        # @TODO Cover new cases
        pytest.skip('Not finished')

        is_called = Event()
        data = ('exchange', 'market', 'symbol', datetime.utcnow(), (), (),)
        factory_fake = FakeTransportFactory()
        factory_fake.consume_price_depth_data = data

        async def cb(*args):
            assert args == data
            is_called.set()

        terminal = factory_terminal(factory_fake)
        keys = []
        await terminal.subscribe_to_prices(cb, keys)
        assert is_called.is_set()


def factory_terminal(
    factory: Optional[TransportFactory] = None,
    depths: Optional[MarketsDepthsBuffer] = None
) -> Terminal:
    if factory is None:
        factory = AsyncMock(spec_set=TransportFactory)
    if depths is None:
        depths = MarketsDepthsBuffer()

    return Terminal(factory, depths)


class FakeTransportFactory(TransportFactory):
    def __init__(self):
        self.get_exchange_entities_data: Optional[Mapping] = None
        self.consume_price_depth_data: Optional[Mapping] = None

    async def get_exchange_entities(
        self,
        on_response: Callable[..., Awaitable]
    ) -> MessageConsumerCollection:
        result = MessageConsumerCollection()
        result.add_consumer(on_response)
        await result.send(self.get_exchange_entities_data)

        return result

    async def consume_price_depth(
        self,
        on_response: Callable[..., Awaitable],
        consume_keys: Optional[List[DepthConsumeKey]] = None
    ) -> MessageConsumerCollection:
        result = MessageConsumerCollection()
        result.add_consumer(on_response)
        await result.send(self.consume_price_depth_data)

        return result


def fixture_no_prices_after_init():
    expected_msg = 'Prices for market with id 1 are unknown'

    yield expected_msg, 'get_depths_of_market', (1,)
    yield expected_msg, 'get_last_depth_of_market', (1,)
    yield expected_msg, 'get_last_side_depth_of_market', (1, DealSide.BUY,)
    yield expected_msg, 'get_last_side_depth_of_market', (1, DealSide.SELL,)


class TestMarketsDepthsBuffer:
    def test_constructor_set_limit(self):
        limit = 10
        prices = MarketsDepthsBuffer(limit)

        assert prices.limit_per_market == limit

    def test_constructor_exception_on_wrong_count_argument(self):
        with pytest.raises(ValueError, match='limit_per_market should be'):
            MarketsDepthsBuffer(0)
        with pytest.raises(ValueError, match='limit_per_market should be'):
            MarketsDepthsBuffer(-5)

    @pytest.mark.parametrize('expected_msg, method_name, args', fixture_no_prices_after_init())
    def test_no_prices_after_init(self, expected_msg: str, method_name: str, args: Tuple[Any]):
        prices = MarketsDepthsBuffer(2)

        with pytest.raises(ValueError, match=expected_msg):
            getattr(prices, method_name)(*args)

    def test_register_depths_appends_to_left(self):
        prices = MarketsDepthsBuffer(2)

        time1 = datetime.utcnow()
        sell_prices1 = ((Decimal('1'), Decimal('2')),)
        buy_prices1 = ((Decimal('3'), Decimal('4')),)
        prices.register_depths(1, time1, sell_prices1, buy_prices1)

        assert list(prices.get_depths_of_market(1)) == [
            (time1, (sell_prices1, buy_prices1,)),
        ]

        time2 = datetime.utcnow()
        sell_prices2 = ((Decimal('2'), Decimal('3')),)
        buy_prices2 = ((Decimal('4'), Decimal('5')),)
        prices.register_depths(1, time2, sell_prices2, buy_prices2)

        assert list(prices.get_depths_of_market(1)) == [
            (time2, (sell_prices2, buy_prices2,)),
            (time1, (sell_prices1, buy_prices1,)),
        ]

        time3 = datetime.utcnow()
        sell_prices3 = ((Decimal('3'), Decimal('4')),)
        buy_prices3 = ((Decimal('5'), Decimal('6')),)
        prices.register_depths(1, time3, sell_prices3, buy_prices3)

        assert list(prices.get_depths_of_market(1)) == [
            (time3, (sell_prices3, buy_prices3,)),
            (time2, (sell_prices2, buy_prices2,)),
        ]

    def test_register_depths_dont_append_duplicates(self):
        prices = MarketsDepthsBuffer(2)

        time1 = datetime.utcnow()
        sell_prices1 = ((Decimal('1'), Decimal('2')),)
        buy_prices1 = ((Decimal('3'), Decimal('4')),)
        prices.register_depths(1, time1, sell_prices1, buy_prices1)

        assert list(prices.get_depths_of_market(1)) == [
            (time1, (sell_prices1, buy_prices1,)),
        ]

        prices.register_depths(1, time1, sell_prices1, buy_prices1)

        assert list(prices.get_depths_of_market(1)) == [
            (time1, (sell_prices1, buy_prices1,)),
        ]

        time2 = datetime.utcnow()
        sell_prices2 = ((Decimal('2'), Decimal('3')),)
        buy_prices2 = ((Decimal('4'), Decimal('5')),)
        prices.register_depths(1, time2, sell_prices2, buy_prices2)

        assert list(prices.get_depths_of_market(1)) == [
            (time2, (sell_prices2, buy_prices2,)),
            (time1, (sell_prices1, buy_prices1,)),
        ]

        prices.register_depths(1, time2, sell_prices2, buy_prices2)

        assert list(prices.get_depths_of_market(1)) == [
            (time2, (sell_prices2, buy_prices2,)),
            (time1, (sell_prices1, buy_prices1,)),
        ]

    def test_get_last_depth_of_market(self):
        prices = MarketsDepthsBuffer(2)

        time1 = datetime.utcnow()
        sell_prices1 = ((Decimal('1'), Decimal('2')),)
        buy_prices1 = ((Decimal('3'), Decimal('4')),)
        prices.register_depths(1, time1, sell_prices1, buy_prices1)

        assert prices.get_last_depth_of_market(1) == (time1, (sell_prices1, buy_prices1,))

        time2 = datetime.utcnow()
        sell_prices2 = ((Decimal('3'), Decimal('4')),)
        buy_prices2 = ((Decimal('5'), Decimal('6')),)
        prices.register_depths(1, time2, sell_prices2, buy_prices2)

        assert prices.get_last_depth_of_market(1) == (time2, (sell_prices2, buy_prices2,))

    def test_get_last_side_depth_of_market_exception_for_unknown_side(self):
        prices = MarketsDepthsBuffer(2)
        prices.register_depths(1, datetime.utcnow(), (), ())

        with pytest.raises(ValueError, match=' for deal side 100500'):
            prices.get_last_side_depth_of_market(1, 100500)

    def test_get_last_side_depth_of_market_returns_side_prices(self):
        prices = MarketsDepthsBuffer(2)

        time1 = datetime.utcnow()
        sell_prices1 = ((Decimal('1'), Decimal('2')),)
        buy_prices1 = ((Decimal('3'), Decimal('4')),)
        prices.register_depths(1, time1, sell_prices1, buy_prices1)

        assert prices.get_last_side_depth_of_market(1, DealSide.BUY) == (time1, buy_prices1,)
        assert prices.get_last_side_depth_of_market(1, DealSide.SELL) == (time1, sell_prices1,)

        time2 = datetime.utcnow()
        sell_prices2 = ((Decimal('3'), Decimal('4'),),)
        buy_prices2 = ((Decimal('5'), Decimal('6'),),)
        prices.register_depths(1, time2, sell_prices2, buy_prices2)

        assert prices.get_last_side_depth_of_market(1, DealSide.BUY) == (time2, buy_prices2,)
        assert prices.get_last_side_depth_of_market(1, DealSide.SELL) == (time2, sell_prices2,)

    def test_are_depths_of_markets_known(self):
        prices = MarketsDepthsBuffer(2)

        assert not prices.are_depths_of_markets_known(1)
        assert not prices.are_depths_of_markets_known(1, 2)

        prices.register_depths(1, datetime.utcnow(), (), ())

        assert prices.are_depths_of_markets_known(1)
        assert not prices.are_depths_of_markets_known(1, 2)

        prices.register_depths(2, datetime.utcnow(), (), ())

        assert prices.are_depths_of_markets_known(1)
        assert prices.are_depths_of_markets_known(1, 2)
