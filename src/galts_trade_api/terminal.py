from asyncio import Event, wait_for
from typing import Dict, MutableMapping

from .asset import Asset, Symbol
from .exchange import Exchange, Market
from .transport import TransportFactory


class Terminal:
    def __init__(self, transport: TransportFactory):
        self._transport_factory: TransportFactory = transport
        self._exchange_entities_inited = Event()
        self._exchanges: Dict[str, Exchange] = {}
        self._assets: Dict[str, Asset] = {}
        self._symbols: Dict[int, Symbol] = {}

    @property
    def transport_factory(self):
        return self._transport_factory

    @transport_factory.setter
    def transport_factory(self, value):
        self._transport_factory = value

    async def init_transport(self) -> None:
        await self.transport_factory.init()

    def shutdown_transport(self) -> None:
        self.transport_factory.shutdown()

    def is_exchange_entities_inited(self) -> bool:
        return self._exchange_entities_inited.is_set()

    async def wait_exchange_entities_inited(self, timeout: float = 5.0) -> None:
        await wait_for(self._exchange_entities_inited.wait(), timeout)

    async def init_exchange_entities(self) -> None:
        await self.transport_factory.init_exchange_entities(
            self._on_init_exchange_entities_response
        )

    async def _on_init_exchange_entities_response(
        self,
        data: MutableMapping[str, MutableMapping]
    ) -> None:
        properties_to_fill = ('exchanges', 'markets', 'symbols', 'assets')

        for prop in properties_to_fill:
            if prop not in data:
                # @TODO Stop the process on this exception
                raise KeyError(f'init_exchange_entities data have not required key "{prop}"')

            data[prop] = {k: v for k, v in data[prop].items() if not v['delete_time']}

        for data in data['assets'].values():
            key = data['tag']
            if key in self._assets:
                raise ValueError(f'Asset with tag "{key}" already exists')

            self._assets[key] = Asset(self.transport_factory, **data)

        for id_, data in data['symbols'].items():
            if id_ in self._symbols:
                raise ValueError(f'Symbol with id {id_} already exists')

            self._symbols[id_] = Symbol(self.transport_factory, **data)

        exchanges_ids_map = {}
        for data in data['exchanges'].values():
            key = data['tag']
            if key in self._exchanges:
                raise ValueError(f'Exchange with tag "{key}" already exists')

            exchange = Exchange(self.transport_factory, **data)
            self._exchanges[key] = exchange
            exchanges_ids_map[data['id']] = exchange

        for data in data['markets'].values():
            key = data['exchange_id']
            if key not in exchanges_ids_map:
                raise ValueError(
                    f'No exchange with id {key} has been found for market with id {data["id"]}'
                )

            exchanges_ids_map[key].add_market(Market(self.transport_factory, **data))

        self._exchange_entities_inited.set()

    async def auth_user(self, username: str, password: str) -> bool:
        return True

    def get_exchange(self, tag: str) -> Exchange:
        return self._exchanges[tag]
