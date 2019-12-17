import uuid
from datetime import datetime
from typing import Dict, Tuple, Type
from unittest import mock
from unittest.mock import call

import grpc
import grpc_testing
import pytest
from google.protobuf.pyext._message import MethodDescriptor

import galts_trade_api.transport.protos.exchange_info.api_pb2 as exchange_info_api
from galts_trade_api.transport.exchange_info_client import ExchangeInfoClient
from galts_trade_api.transport.grpc_utils import message_to_dict
from tests.utils import fixture_grpc_error_status_codes, thread_with_timeout


@pytest.fixture(scope='module')
def grpc_channel():
    descriptors = [exchange_info_api.DESCRIPTOR.services_by_name['ExchangeInfoApi']]
    return grpc_testing.channel(descriptors, grpc_testing.strict_real_time())


def fixture_incorrect_factory_dsn():
    exception_text = 'Parameter is not specified: dsn'

    yield '', ValueError, exception_text
    yield None, ValueError, exception_text
    yield 123, AttributeError, None


def fixture_get_entities():
    some_datetime = datetime(2019, 7, 17, 17, 34, 22, 436922)

    request_expected = exchange_info_api.EntityKinds()
    request_expected.entity_kinds.extend(
        [
            exchange_info_api.ASSET,
            exchange_info_api.SYMBOL,
            exchange_info_api.MARKET,
            exchange_info_api.EXCHANGE
        ]
    )

    response = exchange_info_api.Entities()
    for entity in request_expected.entity_kinds:
        if entity == exchange_info_api.ASSET:
            response.assets[1].id = 1
            response.assets[1].tag = 'BTC'
            response.assets[1].create_time.FromDatetime(some_datetime)
            response.assets[1].delete_time.FromDatetime(some_datetime)
            response.assets[1].name = 'BTC'
            response.assets[1].precision = 8

        if entity == exchange_info_api.SYMBOL:
            response.symbols[1].id = 1
            response.symbols[1].create_time.FromDatetime(some_datetime)
            response.symbols[1].delete_time.FromDatetime(some_datetime)
            response.symbols[1].base_asset_id = 1
            response.symbols[1].quote_asset_id = 2

        if entity == exchange_info_api.MARKET:
            response.markets[1].id = 1
            response.markets[1].custom_tag = 'BTCUSD'
            response.markets[1].create_time.FromDatetime(some_datetime)
            response.markets[1].delete_time.FromDatetime(some_datetime)
            response.markets[1].symbol_id = 1
            response.markets[1].exchange_id = 1
            response.markets[1].trade_endpoint = 'trade_endpoint'

        if entity == exchange_info_api.EXCHANGE:
            response.exchanges[1].id = 1
            response.exchanges[1].tag = 'binance'
            response.exchanges[1].create_time.FromDatetime(some_datetime)
            response.exchanges[1].delete_time.FromDatetime(some_datetime)
            response.exchanges[1].name = 'binance'
            response.exchanges[1].disable_time.FromDatetime(some_datetime)

    result_expected = message_to_dict(response)

    request_id = uuid.uuid4().hex

    method_inputs = (request_id,)

    yield method_inputs, request_expected, response, result_expected, request_id


def fixture_get_entities_failed():
    request_expected = exchange_info_api.EntityKinds()
    request_expected.entity_kinds.extend(
        [
            exchange_info_api.ASSET,
            exchange_info_api.SYMBOL,
            exchange_info_api.MARKET,
            exchange_info_api.EXCHANGE
        ]
    )
    request_id = uuid.uuid4().hex
    method_inputs = (request_id,)

    yield from fixture_grpc_error_status_codes(method_inputs, request_expected, request_id)


class TestSymbolClient:
    @staticmethod
    def test_client_init(grpc_channel: grpc_testing.Channel):
        patch_taget = 'galts_trade_api.transport.exchange_info_client.correct_timeout'
        with mock.patch(patch_taget) as correct_timeout_mock:
            correct_timeout_mock.return_value = 1

            client = ExchangeInfoClient(grpc_channel, timeout_get_entities=1)

            assert correct_timeout_mock.mock_calls == [call(1)]
            assert getattr(client, 'timeout_get_entities') == 1

    @staticmethod
    def test_factory():
        patch_taget = 'galts_trade_api.transport.exchange_info_client.correct_timeout'
        with mock.patch(patch_taget) as correct_timeout_mock:
            correct_timeout_mock.return_value = 1

            client = ExchangeInfoClient.factory('DSN', timeout_get_entities=1)

            assert correct_timeout_mock.mock_calls == [call(1)]
            assert getattr(client, 'timeout_get_entities') == 1

    @staticmethod
    @pytest.mark.parametrize(
        'incorrect_dsn, exception_expected, exception_text',
        fixture_incorrect_factory_dsn()
    )
    def test_incorrect_factory_dsn(
        incorrect_dsn: str,
        exception_expected: Type[Exception],
        exception_text: str
    ):
        with pytest.raises(exception_expected, match=exception_text):
            ExchangeInfoClient.factory(incorrect_dsn)

    @staticmethod
    def test_destroy():
        mock_channel = mock.MagicMock(spec_set=grpc.Channel)

        client = ExchangeInfoClient(mock_channel)
        client.destroy()

        mock_channel.close.assert_called_once()

    @staticmethod
    @pytest.mark.parametrize(
        'method_inputs, request_expected, response, result_expected, request_id_expected',
        fixture_get_entities()
    )
    def test_get_entities(
        grpc_channel: grpc_testing.Channel,
        method_inputs: Tuple,
        response: exchange_info_api.Entities,
        request_expected: exchange_info_api.EntityKinds,
        result_expected: Dict,
        request_id_expected: str
    ):
        client = ExchangeInfoClient(grpc_channel, timeout_get_entities=1)

        result = None

        def sut() -> None:
            nonlocal result
            result = client.get_entities(*method_inputs)

        with thread_with_timeout(sut):
            invocation_metadata, request, channel_rpc = grpc_channel.take_unary_unary(
                get_method_descriptor('GetEntities')
            )

            channel_rpc.terminate(response, None, grpc.StatusCode.OK, None)

        assert request == request_expected
        assert dict(invocation_metadata)['request_id'] == request_id_expected
        assert result == result_expected

    @staticmethod
    @pytest.mark.parametrize(
        'status_code_expected, method_inputs, request_expected, request_id_expected',
        fixture_get_entities_failed()
    )
    def test_get_entities_failed(
        grpc_channel: grpc_testing.Channel,
        status_code_expected: grpc.StatusCode,
        method_inputs: Tuple,
        request_expected: exchange_info_api.EntityKinds,
        request_id_expected: str
    ):
        client = ExchangeInfoClient(grpc_channel, timeout_get_entities=1)

        def sut() -> None:
            client.get_entities(*method_inputs)

        with pytest.raises(grpc.RpcError) as exc_info:
            with thread_with_timeout(sut):
                invocation_metadata, request, channel_rpc = grpc_channel.take_unary_unary(
                    get_method_descriptor('GetEntities')
                )

                channel_rpc.terminate(None, None, status_code_expected, None)

        assert exc_info.value.code() == status_code_expected
        assert request == request_expected
        assert dict(invocation_metadata)['request_id'] == request_id_expected


def get_method_descriptor(method_name: str) -> MethodDescriptor:
    method_descriptor = exchange_info_api.DESCRIPTOR \
        .services_by_name['ExchangeInfoApi'] \
        .methods_by_name[method_name]

    return method_descriptor

# @TODO Catch and fix flanky test fail because of an exception in a thread
