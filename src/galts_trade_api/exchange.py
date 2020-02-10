from __future__ import annotations

import datetime
from typing import Dict, Optional


class Exchange:
    def __init__(
        self,
        id: int,
        tag: str,
        name: str,
        create_time: datetime.datetime,
        delete_time: Optional[datetime.datetime],
        disable_time: Optional[datetime.datetime]
    ):
        self._id: int = int(id)
        self._tag: str = str(tag).strip()
        self._name: str = str(name).strip()
        self._create_time: datetime.datetime = create_time
        self._delete_time: Optional[datetime.datetime] = delete_time
        self._disable_time: Optional[datetime.datetime] = disable_time

        self._markets_by_id: Dict[int, Market] = {}
        self._markets_by_tag: Dict[str, Market] = {}

    @property
    def id(self):
        return self._id

    @property
    def tag(self):
        return self._tag

    @property
    def name(self):
        return self._name

    @property
    def create_time(self):
        return self._create_time

    @property
    def delete_time(self):
        return self._delete_time

    @property
    def disable_time(self):
        return self._disable_time

    @property
    def markets_by_id(self):
        return self._markets_by_id

    @property
    def markets_by_tag(self):
        return self._markets_by_tag

    def add_market(self, market: Market) -> None:
        if market.id in self._markets_by_id:
            raise ValueError(f'Market with id {market.id} already exists in exchange {self.tag}')
        if market.custom_tag in self._markets_by_tag:
            raise ValueError(
                f'Market with tag "{market.custom_tag}" already exists in exchange {self.tag}'
            )

        self._markets_by_id[market.id] = market
        self._markets_by_tag[market.custom_tag] = market


class Market:
    def __init__(
        self,
        id: int,
        custom_tag: str,
        exchange_id: int,
        symbol_id: int,
        trade_endpoint: str,
        create_time: datetime.datetime,
        delete_time: Optional[datetime.datetime]
    ):
        self._id: int = int(id)
        self._custom_tag: str = str(custom_tag).strip()
        self._exchange_id: int = int(exchange_id)
        self._symbol_id: int = int(symbol_id)
        self._trade_endpoint: str = str(trade_endpoint).strip()
        self._create_time: datetime.datetime = create_time
        self._delete_time: Optional[datetime.datetime] = delete_time

    @property
    def id(self):
        return self._id

    @property
    def custom_tag(self):
        return self._custom_tag

    @property
    def exchange_id(self):
        return self._exchange_id

    @property
    def symbol_id(self):
        return self._symbol_id

    @property
    def trade_endpoint(self):
        return self._trade_endpoint

    @property
    def create_time(self):
        return self._create_time

    @property
    def delete_time(self):
        return self._delete_time
