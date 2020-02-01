import asyncio
import datetime
import json
from dataclasses import dataclass
from functools import partial
from multiprocessing import Event, Pipe, Process
from multiprocessing.connection import Connection
from typing import Any, Awaitable, Callable, Dict, List, MutableMapping, Optional, Type

import aio_pika

from . import DepthConsumeKey, MessageConsumerCollection, PipeRequest, PipeResponseRouter, \
    TransportFactory, TransportFactoryException
from .exchange_info_client import ExchangeInfoClient
from .grpc_utils import generate_request_id
from ..asyncio_helper import AsyncProgramEnv, run_program_forever
from ..tools import Singleton

AioPikaConsumeCallable = Callable[[aio_pika.IncomingMessage], Any]


@dataclass(frozen=True)
class GetExchangeEntitiesRequest(PipeRequest):
    dsn: str
    timeout: float


@dataclass(frozen=True)
class ConsumePriceDepthRequest(PipeRequest):
    dsn: str
    exchange: str
    consume_keys: frozenset


class RabbitConnection(
    metaclass=Singleton,
    singleton_hash_args=[0],
    singleton_hash_kwargs=['dsn']
):
    def __init__(self, dsn: str):
        self._dsn: str = str(dsn).strip()
        self._connection: Optional[aio_pika.Connection] = None

    @property
    def connection(self):
        return self._connection

    async def create_channel(self, prefetch_count: int = 100) -> aio_pika.Channel:
        if self._connection is None:
            self._connection = await aio_pika.connect_robust(self._dsn)

        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=prefetch_count)

        return channel


class RabbitConsumer:
    def __init__(
        self,
        channel: aio_pika.Channel,
        exchange_name: str,
        on_message: AioPikaConsumeCallable
    ):
        self._channel = channel
        self._exchange_name = str(exchange_name).strip()
        self._on_message = on_message
        self._exchange: Optional[aio_pika.Exchange] = None
        self._queue: Optional[aio_pika.Queue] = None

    @property
    def channel(self):
        return self._channel

    @property
    def exchange_name(self):
        return self._exchange_name

    @property
    def exchange(self):
        return self._exchange

    @property
    def queue(self):
        return self._queue

    async def create_queue(self) -> aio_pika.Queue:
        self._exchange = await self.channel.declare_exchange(self._exchange_name, passive=True)
        self._queue = await self.channel.declare_queue(exclusive=True)

        await self.queue.consume(self._on_message, no_ack=True)

        return self.queue


class RealTransportFactory(TransportFactory):
    def __init__(
        self,
        exchange_info_dsn: str,
        depth_scraping_queue_dsn: str,
        depth_scraping_queue_exchange: str,
        exchange_info_get_entities_timeout: float = 5.0,
        process_ready_timeout: float = 2.0,
    ):
        super().__init__()
        self._process: Optional[Process] = None
        self._process_ready = Event()
        self._parent_connection, self._child_connection = Pipe()
        self._response_router: Optional[PipeResponseRouter] = None

        def sanity_string(value: str) -> str: return str(value).strip()

        self._exchange_info_dsn = sanity_string(exchange_info_dsn)
        self._exchange_info_get_entities_timeout = float(exchange_info_get_entities_timeout)
        self._depth_scraping_queue_dsn = sanity_string(depth_scraping_queue_dsn)
        self._depth_scraping_queue_exchange = sanity_string(depth_scraping_queue_exchange)
        self._process_ready_timeout = float(process_ready_timeout)

    @property
    def exchange_info_dsn(self):
        return self._exchange_info_dsn

    @property
    def exchange_info_get_entities_timeout(self):
        return self._exchange_info_get_entities_timeout

    @property
    def depth_scraping_queue_dsn(self):
        return self._depth_scraping_queue_dsn

    @property
    def depth_scraping_queue_exchange(self):
        return self._depth_scraping_queue_exchange

    @property
    def process_ready_timeout(self):
        return self._process_ready_timeout

    async def init(self, loop_debug: Optional[bool] = None) -> None:
        if self._process:
            raise RuntimeError('A process for RealTransportFactory should be created only once')

        self._process = RealTransportProcess(
            loop_debug=loop_debug,
            ready_event=self._process_ready,
            connection=self._child_connection
        )
        self._process.start()

        if not self._process_ready.wait(self.process_ready_timeout):
            raise RuntimeError('Failed to initialize RealTransportFactory in time')

        self._response_router = PipeResponseRouter(self._parent_connection)
        task = asyncio.create_task(self._response_router.start())

        def task_done_cb(t: asyncio.Task) -> None:
            if t.cancelled():
                self.shutdown()
                return

            if t.exception():
                self.shutdown()
                raise t.exception()

        task.add_done_callback(task_done_cb)

    def shutdown(self) -> None:
        if self._process:
            self._process.terminate()

    async def get_exchange_entities(
        self,
        on_response: Callable[..., Awaitable]
    ) -> MessageConsumerCollection:
        request = GetExchangeEntitiesRequest(
            self.exchange_info_dsn,
            self.exchange_info_get_entities_timeout
        )
        result = self._response_router.prepare_consumers_of_response(request)
        result.add_consumer(on_response)
        self._parent_connection.send(request)

        return result

    async def consume_price_depth(
        self,
        on_response: Callable[..., Awaitable],
        consume_keys: Optional[List[DepthConsumeKey]] = None
    ) -> MessageConsumerCollection:
        request = ConsumePriceDepthRequest(
            self.depth_scraping_queue_dsn,
            self.depth_scraping_queue_exchange,
            frozenset(consume_keys)
        )
        result = self._response_router.prepare_consumers_of_response(request)
        result.add_consumer(on_response)
        self._parent_connection.send(request)

        return result


class RealTransportProcess(Process):
    def __init__(
        self,
        *args,
        loop_debug: Optional[bool] = None,
        ready_event: Event,
        connection: Connection,
        poll_delay: float = 0.001,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._loop_debug = loop_debug
        self._ready_event = ready_event
        self._connection = connection
        self._poll_delay = float(poll_delay)
        self._handlers: MutableMapping[Type[PipeRequest], Callable[..., Awaitable]] = {}

        self.add_handler(GetExchangeEntitiesRequest, self._get_exchange_entities)
        self.add_handler(ConsumePriceDepthRequest, self._consume_price_depth)

    @property
    def poll_delay(self):
        return self._poll_delay

    def add_handler(
        self,
        request_type: Type[PipeRequest],
        handler: Callable[..., Awaitable]
    ) -> None:
        if request_type in self._handlers:
            raise ValueError(f'Handler for {request_type} already registered')

        self._handlers[request_type] = handler

    def run(self) -> None:
        run_program_forever(self.main, loop_debug=self._loop_debug)

    async def main(self, program_env: AsyncProgramEnv) -> None:
        def exception_handler(_, context: Dict) -> None:
            if 'exception' in context:
                self._notify_owner_process(context['exception'])

        program_env.exception_handler_patch = exception_handler

        self._ready_event.set()

        while True:
            if not self._connection.poll():
                await asyncio.sleep(self.poll_delay)
                continue

            request = self._connection.recv()

            handler = self._find_handler_for_request(request)
            # @TODO The tasks should be collected for cancellation when this main task has been
            # cancelled. But when I've tried to done this I've got Segmentation fault from
            # unit-tests. May be it depends on Python version.
            asyncio.create_task(handler(request))

    async def _get_exchange_entities(self, request: GetExchangeEntitiesRequest) -> None:
        client = ExchangeInfoClient.factory(request.dsn, timeout_get_entities=request.timeout)
        entities = client.get_entities(generate_request_id())
        self._respond_to_owner_request(request, entities)

        client.destroy()

    async def _consume_price_depth(self, request: ConsumePriceDepthRequest) -> None:
        connection = RabbitConnection(request.dsn)
        channel = await connection.create_channel(100)
        cb = partial(self._price_depth_callback, request)
        consumer = RabbitConsumer(channel, request.exchange, cb)
        queue = await consumer.create_queue()

        if not request.consume_keys:
            routing_keys = ['#']
        else:
            routing_keys = [k.format_for_rabbitmq() for k in request.consume_keys]

        for routing_key in routing_keys:
            await queue.bind(consumer.exchange, routing_key)

    def _respond_to_owner_request(self, request: PipeRequest, content: Any) -> None:
        self._connection.send([request, content])

    def _notify_owner_process(self, original_exception: Exception) -> None:
        # This wrapping is required to don't fire unnecessary errors about serialization
        # of the exception. Otherwise a framework user will see undesired spam about
        # pickling RLock etc in logs.
        wrapped_exception = TransportFactoryException('An error in the transport process')
        # Unfortunately this line has no side-effects in Python 3.7 because this attribute
        # won't be packed by Connection.send() therefore cross-Connection.recv() will unpack
        # the data without the value for this attribute. So we don't have real wrapping here.
        wrapped_exception.__cause__ = original_exception
        self._connection.send([wrapped_exception])

    def _find_handler_for_request(self, request: PipeRequest) -> Callable[..., Awaitable]:
        request_type = type(request)
        if request_type not in self._handlers:
            raise ValueError(f'No handler for request type {request_type}')

        return self._handlers[request_type]

    def _price_depth_callback(
        self,
        request: ConsumePriceDepthRequest,
        message: aio_pika.IncomingMessage
    ) -> None:
        body = json.loads(message.body)
        args = [
            body['exchange'],
            body['market'],
            body['symbol'],
            datetime.datetime.fromisoformat(body['now']),
            body['depth']['bids'],
            body['depth']['asks'],
        ]

        self._respond_to_owner_request(request, args)
