import datetime
from typing import Any, Optional

import pytest

from galts_trade_api.exchange import Exchange, Market


def fixture_exchange_constructor_cast_properties():
    # String properties
    for prop in ('tag', 'name',):
        yield prop, 1, '1'
        yield prop, 2.0, '2.0'
        yield prop, False, 'False'
        yield prop, ' test ', 'test'

    # Integer properties
    for prop in ('id',):
        yield prop, '-1', -1
        yield prop, '1', 1
        yield prop, 2.0, 2

    # Time properties
    for prop in ('create_time', 'delete_time', 'disable_time',):
        now = datetime.datetime.now()
        yield prop, now, now


def factory_add_market_exception_on_duplicate():
    yield factory_market(), factory_market(), f'Market with id 1 already exists'
    yield factory_market(), factory_market(id=2), f'Market with tag "tag" already exists'


class TestExchange:
    @pytest.mark.parametrize(
        'prop, arg_value, expected_value',
        fixture_exchange_constructor_cast_properties()
    )
    def test_constructor_cast_properties(self, prop: str, arg_value: Any, expected_value: Any):
        instance = self._factory_exchange(**{prop: arg_value})

        assert getattr(instance, prop) == expected_value

    @pytest.mark.parametrize(
        'market1, market2, exception_msg',
        factory_add_market_exception_on_duplicate()
    )
    def test_add_market_exception_on_duplicate(
        self,
        market1: Market,
        market2: Market,
        exception_msg: str
    ):
        exchange = self._factory_exchange()
        exchange.add_market(market1)

        with pytest.raises(ValueError, match=exception_msg):
            exchange.add_market(market2)

    def test_markets_by_tag(self):
        market_tag = 'market-tag'
        exchange = self._factory_exchange()
        market = factory_market(custom_tag=market_tag)

        exchange.add_market(market)

        assert exchange.markets_by_tag[market_tag] is market

    @classmethod
    def _factory_exchange(
        cls,
        id: Optional[int] = 1,
        tag: Optional[str] = 'tag',
        name: Optional[str] = 'name',
        create_time: Optional[datetime.datetime] = datetime.datetime.now(),
        delete_time: Optional[datetime.datetime] = datetime.datetime.now(),
        disable_time: Optional[datetime.datetime] = datetime.datetime.now(),
    ) -> Exchange:
        return Exchange(id, tag, name, create_time, delete_time, disable_time)


def fixture_market_constructor_cast_properties():
    # String properties
    for prop in ('custom_tag', 'trade_endpoint',):
        yield prop, 1, '1'
        yield prop, 2.0, '2.0'
        yield prop, False, 'False'
        yield prop, ' test ', 'test'

    # Integer properties
    for prop in ('id', 'exchange_id', 'symbol_id',):
        yield prop, '-1', -1
        yield prop, '1', 1
        yield prop, 2.0, 2

    # Time properties
    for prop in ('create_time', 'delete_time',):
        now = datetime.datetime.now()
        yield prop, now, now


class TestMarket:
    @pytest.mark.parametrize(
        'prop, arg_value, expected_value',
        fixture_market_constructor_cast_properties()
    )
    def test_constructor_cast_properties(self, prop: str, arg_value: Any, expected_value: Any):
        instance = factory_market(**{prop: arg_value})

        assert getattr(instance, prop) == expected_value


def factory_market(
    id: Optional[int] = 1,
    custom_tag: Optional[str] = 'tag',
    exchange_id: Optional[int] = 3,
    symbol_id: Optional[int] = 4,
    trade_endpoint: Optional[str] = 'test.local',
    create_time: Optional[datetime.datetime] = datetime.datetime.now(),
    delete_time: Optional[datetime.datetime] = datetime.datetime.now(),
) -> Market:
    return Market(id, custom_tag, exchange_id, symbol_id, trade_endpoint, create_time, delete_time)
