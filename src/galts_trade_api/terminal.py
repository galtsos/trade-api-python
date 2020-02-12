import datetime
from asyncio import Event, wait_for
from decimal import Decimal
from typing import Awaitable, Callable, Collection, Dict, List, Mapping, MutableMapping, Optional, \
    Tuple, Union

from .asset import Asset, Symbol
from .exchange import Exchange, Market
from .tools import find_duplicates_in_list
from .transport import DepthConsumeKey, TransportFactory

PriceLevelWithoutFee = Tuple[Decimal, Decimal]
PriceLevelWithFee = Tuple[Decimal, Decimal, Optional[Decimal]]
PriceLevel = Union[PriceLevelWithoutFee, PriceLevelWithFee]
PriceDepth = List[PriceLevel]

OnPriceCallable = Callable[[str, str, str, datetime.datetime, PriceDepth, PriceDepth], Awaitable]


class Terminal:
    def __init__(self, transport: TransportFactory):
        self._transport_factory: TransportFactory = transport
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
        await self.transport_factory.consume_price_depth(
            lambda event: callback(*event),
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
