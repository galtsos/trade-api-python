import asyncio
from abc import ABC, abstractmethod
from collections.abc import Sequence
from multiprocessing.connection import Connection
from typing import Any, Awaitable, Callable, List, MutableMapping, NamedTuple, Optional, Set

OnExceptionCallable = Callable[[BaseException], Any]


class DepthConsumeKey(NamedTuple):
    exchange_tag: str = '*'
    market_tag: str = '*'
    symbol_tag: str = '*'

    def format_for_rabbitmq(self) -> str:
        required_fields = ('exchange_tag', 'market_tag', 'symbol_tag')

        for arg in required_fields:
            if not len(getattr(self, arg).strip()):
                raise ValueError(f'Field {arg} should be non-empty string')

        if self.exchange_tag == self.market_tag == self.symbol_tag == '*':
            return '#'

        return f'{self.exchange_tag}.{self.market_tag}.{self.symbol_tag}'


class MessageConsumerCollection:
    def __init__(self):
        self._consumers: Set[Callable[..., Awaitable]] = set()

    def add_consumer(self, callback: Callable[..., Awaitable]) -> None:
        self._consumers.add(callback)

    async def send(self, data: Any) -> None:
        coroutines = [consumer(data) for consumer in self._consumers]

        tasks_result = await asyncio.gather(*coroutines, return_exceptions=True)

        for result in tasks_result:
            if isinstance(result, BaseException):
                raise result


class PipeRequest:
    """
    Each subclass of this should be decorated by @dataclass(frozen=True) otherwise
    PipeResponseRouter will fail to distinct request's objects from each other and to select
    an appropriate MessageConsumerCollection object.
    """


class PipeResponseRouter:
    def __init__(self, connection: Connection, poll_delay: float = 0.001):
        self._connection = connection
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

            if len(message) == 1 and isinstance(message[0], Exception):
                raise message[0]

            if len(message) != 2:
                raise ValueError('Pipe response message should contain exactly 2 elements')

            request, response = message
            self._dispatch(request, response)

    def prepare_consumers_of_response(self, request: PipeRequest) -> MessageConsumerCollection:
        if request not in self._consumers:
            self._consumers[request] = MessageConsumerCollection()

        return self._consumers[request]

    def _dispatch(self, request: PipeRequest, response: Any) -> None:
        if request not in self._consumers:
            return

        # Don't store the sub-tasks because we don't want to cancel them if self.start() has been
        # cancelled.
        asyncio.create_task(self._consumers[request].send(response))


class TransportFactory(ABC):
    async def init(self, loop_debug: Optional[bool] = None) -> None:
        pass

    def shutdown(self) -> None:
        pass

    @abstractmethod
    async def get_exchange_entities(
        self,
        on_response: Callable[..., Awaitable]
    ) -> MessageConsumerCollection:
        pass

    @abstractmethod
    async def consume_price_depth(
        self,
        on_response: Callable[..., Awaitable],
        consume_keys: Optional[List[DepthConsumeKey]] = None
    ) -> MessageConsumerCollection:
        pass


class TransportFactoryException(Exception): pass
