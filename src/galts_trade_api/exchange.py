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

        self._markets: Dict[str, Market] = {}

    @property
    def tag(self):
        return self._tag

    def add_market(self, market: Market) -> None:
        if market.custom_tag in self._markets:
            raise ValueError(
                f'Market with tag "{market.custom_tag}" already exists in exchange {self.tag}'
            )

        self._markets[market.custom_tag] = market

    def get_market_by_custom_tag(self, custom_tag: str) -> Market:
        return self._markets[custom_tag]


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
    def custom_tag(self):
        return self._custom_tag
