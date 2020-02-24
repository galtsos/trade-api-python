from __future__ import annotations

import datetime
from asyncio import Event, wait_for
from collections import deque
from copy import copy
from decimal import Decimal
from typing import Awaitable, Callable, Collection, Deque, Dict, Mapping, MutableMapping, \
    Optional, Tuple, Union

from . import logger
from .asset import Asset, DealSide, Symbol
from .exchange import Exchange, Market
from .tools import find_duplicates_in_list
from .transport import DepthConsumeKey, TransportFactory

PriceLevelWithoutFee = Tuple[Decimal, Decimal]
PriceLevelWithFee = Tuple[Decimal, Decimal, Optional[Decimal]]
PriceLevel = Union[PriceLevelWithoutFee, PriceLevelWithFee]
PriceDepth = Tuple[PriceLevel]
FullDepth = Tuple[PriceDepth, PriceDepth]

OnPriceCallable = Callable[[str, str, str, datetime.datetime, PriceDepth, PriceDepth], Awaitable]


class Terminal:
    @classmethod
    def factory(cls, transport: TransportFactory, depths_limit_per_market: int = 1) -> Terminal:
        depths = MarketsDepthsBuffer(depths_limit_per_market)

        return cls(transport, depths)

    def __init__(self, transport: TransportFactory, depths: MarketsDepthsBuffer):
        self._transport_factory: TransportFactory = transport
        self._depths: MarketsDepthsBuffer = depths

        self._exchange_entities_inited = Event()
        self._assets_by_id: Dict[int, Asset] = {}
        self._assets_by_tag: Dict[str, Asset] = {}
        self._symbols_by_id: Dict[int, Symbol] = {}
        self._symbols_by_tag: Dict[str, Symbol] = {}
        self._exchanges_by_id: Dict[int, Exchange] = {}
        self._exchanges_by_tag: Dict[str, Exchange] = {}

    @property
    def transport_factory(self):
        return self._transport_factory

    @transport_factory.setter
    def transport_factory(self, value: TransportFactory):
        self._transport_factory = value

    @property
    def depths(self):
        return self._depths

    @property
    def assets_by_id(self):
        return self._assets_by_id

    @property
    def assets_by_tag(self):
        return self._assets_by_tag

    @property
    def symbols_by_id(self):
        return self._symbols_by_id

    @property
    def symbols_by_tag(self):
        return self._symbols_by_tag

    @property
    def exchanges_by_id(self):
        return self._exchanges_by_id

    @property
    def exchanges_by_tag(self):
        return self._exchanges_by_tag

    async def init_transport(self, loop_debug: Optional[bool] = None) -> None:
        await self.transport_factory.init(loop_debug)

    def shutdown_transport(self) -> None:
        self.transport_factory.shutdown()

    async def auth_user(self, username: str, password: str) -> bool:
        return True

    def is_exchange_entities_inited(self) -> bool:
        return self._exchange_entities_inited.is_set()

    async def wait_exchange_entities_inited(self, timeout: float = 5.0) -> None:
        await wait_for(self._exchange_entities_inited.wait(), timeout)

    async def init_exchange_entities(self) -> None:
        await self.transport_factory.get_exchange_entities(
            self._on_init_exchange_entities_response
        )

    async def subscribe_to_prices(
        self,
        callback: OnPriceCallable,
        consume_keys: Optional[Collection[DepthConsumeKey]] = None
    ) -> None:
        updater = depths_updater(self, callback)

        await self.transport_factory.consume_price_depth(
            lambda event: updater(*event),
            consume_keys
        )

    async def _on_init_exchange_entities_response(self, data: MutableMapping[str, Mapping]) -> None:
        properties_to_fill = ('exchanges', 'markets', 'symbols', 'assets')

        for prop in properties_to_fill:
            if prop not in data:
                raise KeyError(f'Key "{prop}" is required')

            data[prop] = {k: v for k, v in data[prop].items() if not v['delete_time']}

        all_assets_tags = [entity['tag'] for entity in data['assets'].values()]
        duplicates = find_duplicates_in_list(all_assets_tags)
        if len(duplicates):
            raise ValueError(f"Assets with duplicates in tags found: {', '.join(duplicates)}")

        for entity in data['assets'].values():
            asset = Asset(**entity)
            self._assets_by_id[entity['id']] = asset
            self._assets_by_tag[entity['tag']] = asset

        for entity in data['symbols'].values():
            if entity['base_asset_id'] not in data['assets']:
                raise ValueError(
                    f"No base asset with id {entity['base_asset_id']} "
                    f"has been found for symbol with id {entity['id']}"
                )
            if entity['quote_asset_id'] not in data['assets']:
                raise ValueError(
                    f"No quote asset with id {entity['quote_asset_id']} "
                    f"has been found for symbol with id {entity['id']}"
                )

            symbol = Symbol(**entity)
            base_asset = self._assets_by_id[symbol.base_asset_id]
            quote_asset = self._assets_by_id[symbol.quote_asset_id]
            tag = Symbol.form_tag(base_asset.tag, quote_asset.tag)

            if tag in self._symbols_by_tag:
                raise ValueError(f'Symbols with duplicates in tags found: {tag}')

            self._symbols_by_id[entity['id']] = symbol
            self._symbols_by_tag[tag] = symbol

        all_exchanges_tags = [entity['tag'] for entity in data['exchanges'].values()]
        duplicates = find_duplicates_in_list(all_exchanges_tags)
        if len(duplicates):
            raise ValueError(f"Exchanges with duplicates in tags found: {', '.join(duplicates)}")

        for entity in data['exchanges'].values():
            exchange = Exchange(**entity)
            self._exchanges_by_id[entity['id']] = exchange
            self._exchanges_by_tag[entity['tag']] = exchange

        for entity in data['markets'].values():
            if entity['exchange_id'] not in self._exchanges_by_id:
                raise ValueError(
                    f"No exchange with id {entity['exchange_id']} "
                    f"has been found for market with id {entity['id']}"
                )
            if entity['symbol_id'] not in data['symbols']:
                raise ValueError(
                    f"No symbol with id {entity['symbol_id']} "
                    f"has been found for market with id {entity['id']}"
                )

            self._exchanges_by_id[entity['exchange_id']].add_market(Market(**entity))

        self._exchange_entities_inited.set()


# Term from https://www.investopedia.com/terms/d/depth-of-market.asp
class MarketsDepthsBuffer:
    def __init__(self, limit_per_market: int = 1):
        self._limit_per_market = int(limit_per_market)
        self._depths: Dict[int, Deque[Tuple[datetime.datetime, FullDepth]]] = {}

        if self.limit_per_market < 1:
            raise ValueError('Value of limit_per_market should be equal to or greater than 1')

    @property
    def limit_per_market(self):
        return self._limit_per_market

    def register_depths(
        self,
        market_id: int,
        time: datetime.datetime,
        bids: PriceDepth,
        asks: PriceDepth
    ) -> None:
        if market_id not in self._depths:
            self._depths[market_id] = deque(maxlen=self.limit_per_market)

        record = (time, (bids, asks,),)

        if len(self._depths[market_id]) > 0 and self._depths[market_id][0] == record:
            return

        self._depths[market_id].appendleft(record)

    def get_depths_of_market(self, market_id: int) -> Deque[Tuple[datetime.datetime, FullDepth]]:
        if not self.are_depths_of_markets_known(market_id):
            raise ValueError(f'Prices for market with id {market_id} are unknown')

        return self._depths[market_id]

    def get_last_depth_of_market(self, market_id: int) -> Tuple[datetime.datetime, FullDepth]:
        return self.get_depths_of_market(market_id)[0]

    def get_last_side_depth_of_market(
        self,
        market_id: int,
        side: DealSide
    ) -> Tuple[datetime.datetime, PriceDepth]:
        time, last_depth = self.get_last_depth_of_market(market_id)

        # bids - to sell, asks - to buy
        if side == DealSide.SELL:
            return time, last_depth[0]
        elif side == DealSide.BUY:
            return time, last_depth[1]
        else:
            raise ValueError(f'Cannot get part of full depth record for deal side {side}')

    def are_depths_of_markets_known(self, *market_ids: int) -> bool:
        are_known = [id_ in self._depths for id_ in market_ids]

        return all(are_known)


def depths_updater(terminal: Terminal, callback: OnPriceCallable) -> OnPriceCallable:
    busyness_flag = Event()
    latest_update = []

    async def on_depth_update(
        exchange_tag: str,
        market_tag: str,
        symbol_tag: str,
        time: datetime.datetime,
        bids: PriceDepth,
        asks: PriceDepth
    ) -> None:
        nonlocal latest_update

        try:
            exchange = terminal.exchanges_by_tag[exchange_tag]
            market = exchange.markets_by_tag[market_tag]
            terminal.depths.register_depths(market.id, time, bids, asks)
        except Exception:
            logger.exception('cannot_find_market', exchange_tag=exchange_tag, market_tag=market_tag)

        actual_update = (exchange_tag, market_tag, symbol_tag, time, bids, asks,)
        latest_update = actual_update

        if busyness_flag.is_set():
            logger.debug('Previous depths update callback has not yet completed')
            return

        busyness_flag.set()

        while True:
            await callback(*actual_update)

            if actual_update == latest_update:
                break

            actual_update = copy(latest_update)

        busyness_flag.clear()

    return on_depth_update
