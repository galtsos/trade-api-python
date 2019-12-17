import asyncio
from abc import ABC, abstractmethod
from collections.abc import Sequence
from multiprocessing.connection import Connection
from typing import Any, Awaitable, Callable, List, MutableMapping, NamedTuple, Optional, Set

OnExceptionCallable = Callable[[BaseException], Any]


class DepthConsumeKey(NamedTuple):
    exchange: str = '*'
    market_tag: str = '*'
    symbol_tag: str = '*'

    def format_for_rabbitmq(self) -> str:
        required_fields = ('exchange', 'market_tag', 'symbol_tag')

        for arg in required_fields:
            if not len(getattr(self, arg).strip()):
                raise ValueError(f'Field {arg} should be non-empty string')

        if self.exchange == self.market_tag == self.symbol_tag == '*':
            return '#'

        return f'{self.exchange}.{self.market_tag}.{self.symbol_tag}'


class MessageConsumerCollection:
    def __init__(self):
        self._consumers: Set[Callable[..., Awaitable]] = set()

    def add_consumer(self, callback: Callable[..., Awaitable]) -> None:
        self._consumers.add(callback)

    async def notify(self, data: Any) -> None:
        coroutines = [consumer(data) for consumer in self._consumers]

        await asyncio.gather(*coroutines)


class PipeRequest:
    """
    Each subclass of this should be decorated by @dataclass(frozen=True) otherwise
    PipeResponseRouter will fail to distinct request's objects from each other and to select
    an appropriate MessageConsumerCollection object.
    """


class PipeResponseRouter:
    def __init__(
        self,
        connection: Connection,
        on_exception: OnExceptionCallable,
        poll_delay: float = 0.001
    ):
        self._connection = connection
        self._on_exception = on_exception
        self._poll_delay = float(poll_delay)
        self._consumers: MutableMapping[PipeRequest, MessageConsumerCollection] = {}

    async def start(self) -> None:
        while True:
            if not self._connection.poll():
                await asyncio.sleep(self._poll_delay)
                continue

            message = self._connection.recv()

            if not isinstance(message, Sequence):
                raise ValueError('Pipe response message should be an object with indexing')

            if isinstance(message[0], Exception):
                raise message[0]

            if len(message) != 2:
                raise ValueError('Pipe response message can contain exactly 2 elements')

            request, data = message
            self._notify(request, data)

    def _notify(self, request: PipeRequest, data: Any) -> None:
        if request not in self._consumers:
            return

        task = asyncio.create_task(self._consumers[request].notify(data))

        def task_done_cb(t: asyncio.Task):
            if t.cancelled():
                return

            if t.exception() is not None:
                self._on_exception(t.exception())
                raise t.exception()

        task.add_done_callback(task_done_cb)

    def init_response_consumer(self, request: PipeRequest) -> MessageConsumerCollection:
        if request not in self._consumers:
            self._consumers[request] = MessageConsumerCollection()

        return self._consumers[request]


class TransportFactory(ABC):
    async def init(self, on_exception: OnExceptionCallable) -> None:
        pass

    def shutdown(self) -> None:
        pass

    @abstractmethod
    async def init_exchange_entities(
        self,
        on_response: Callable[..., Awaitable]
    ) -> MessageConsumerCollection:
        pass

    @abstractmethod
    async def get_depth_scraping_consumer(
        self,
        on_response: Callable[..., Awaitable],
        consume_keys: Optional[List[DepthConsumeKey]] = None
    ) -> MessageConsumerCollection:
        pass


class TransportFactoryException(Exception): pass
