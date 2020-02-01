import datetime
from typing import Any, Optional

import pytest

from galts_trade_api.asset import Asset, Symbol


def fixture_asset_constructor_cast_properties():
    # String properties
    for prop in ('tag', 'name',):
        yield prop, 1, '1'
        yield prop, 2.0, '2.0'
        yield prop, False, 'False'
        yield prop, ' test ', 'test'

    # Integer properties
    for prop in ('id', 'precision',):
        yield prop, '-1', -1
        yield prop, '1', 1
        yield prop, 2.0, 2

    # Time properties
    for prop in ('create_time', 'delete_time',):
        now = datetime.datetime.now()
        yield prop, now, now


class TestAsset:
    @pytest.mark.parametrize(
        'prop, arg_value, expected_value',
        fixture_asset_constructor_cast_properties()
    )
    def test_constructor_cast_properties(self, prop: str, arg_value: Any, expected_value: Any):
        instance = self._factory_asset(**{prop: arg_value})

        assert getattr(instance, prop) == expected_value

    @classmethod
    def _factory_asset(
        cls,
        id: Optional[int] = 1,
        tag: Optional[str] = 'tag',
        name: Optional[str] = 'name',
        precision: Optional[int] = 2,
        create_time: Optional[datetime.datetime] = datetime.datetime.now(),
        delete_time: Optional[datetime.datetime] = datetime.datetime.now()
    ) -> Asset:
        return Asset(id, tag, name, precision, create_time, delete_time)


def fixture_symbol_constructor_cast_properties():
    # Integer properties
    for prop in ('id', 'base_asset_id', 'quote_asset_id',):
        yield prop, '-1', -1
        yield prop, '1', 1
        yield prop, 2.0, 2

    # Time properties
    for prop in ('create_time', 'delete_time',):
        now = datetime.datetime.now()
        yield prop, now, now


class TestSymbol:
    @pytest.mark.parametrize(
        'prop, arg_value, expected_value',
        fixture_symbol_constructor_cast_properties()
    )
    def test_constructor_cast_properties(self, prop: str, arg_value, expected_value):
        instance = self._factory_symbol(**{prop: arg_value})

        assert getattr(instance, prop) == expected_value

    @classmethod
    def _factory_symbol(
        cls,
        id: Optional[int] = 1,
        base_asset_id: Optional[int] = 5,
        quote_asset_id: Optional[int] = 6,
        create_time: Optional[datetime.datetime] = datetime.datetime.now(),
        delete_time: Optional[datetime.datetime] = datetime.datetime.now()
    ) -> Symbol:
        return Symbol(id, base_asset_id, quote_asset_id, create_time, delete_time)
