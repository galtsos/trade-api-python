from __future__ import annotations

from typing import Dict

import grpc

from .grpc_utils import OptionalGrpcTimeout, correct_timeout, get_metadata, message_to_dict
from .protos.exchange_info import api_pb2 as exchange_info_api
from .protos.exchange_info.api_pb2_grpc import ExchangeInfoApiStub


class ExchangeInfoClient:
    @classmethod
    def factory(cls, dsn: str, **kwargs) -> ExchangeInfoClient:
        """
        Args:
            kwargs: see keyword arguments in the constructor
        """
        if not dsn:
            raise ValueError(f'Parameter is not specified: dsn')

        channel = grpc.insecure_channel(dsn)

        return cls(channel, **kwargs)

    def __init__(
        self,
        channel: grpc.Channel,
        timeout_get_entities: OptionalGrpcTimeout = None
    ):
        """
        Args:
            timeout_get_entities: grpc timeout in sec.
        """
        self._channel = channel
        self._stub = ExchangeInfoApiStub(channel)
        self._timeout_get_entities = correct_timeout(timeout_get_entities)

    @property
    def timeout_get_entities(self):
        return self._timeout_get_entities

    def destroy(self) -> None:
        if getattr(self, '_channel', None):
            self._channel.close()

    def get_entities(self, request_id: str) -> Dict[str, Dict[int, Dict]]:
        request = exchange_info_api.EntityKinds()
        request.entity_kinds.extend([
            exchange_info_api.ASSET,
            exchange_info_api.SYMBOL,
            exchange_info_api.MARKET,
            exchange_info_api.EXCHANGE
        ])

        response = self._stub.GetEntities(
            request,
            metadata=get_metadata(request_id),
            timeout=self._timeout_get_entities
        )

        return message_to_dict(response)
