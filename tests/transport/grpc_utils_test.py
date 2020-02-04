from typing import Optional

import pytest

from galts_trade_api.transport.grpc_utils import OptionalGrpcTimeout, correct_timeout, \
    generate_request_id, get_metadata


def test_generate_request_id():
    request_id = generate_request_id()

    assert isinstance(request_id, str)
    assert len(request_id) == 32


def test_get_metadata():
    request_id = 'c997bf1f855d47078afe6e5ae0b35e89'

    metadata_with_specified_request_id = [('request_id', request_id)]

    assert get_metadata(request_id) == metadata_with_specified_request_id
    assert get_metadata() != metadata_with_specified_request_id


def fixture_correct_timeout():
    input_timeout1 = None
    timeout_expected1 = None

    input_timeout2 = -1
    timeout_expected2 = None

    input_timeout3 = 0
    timeout_expected3 = None

    input_timeout4 = 1
    timeout_expected4 = 1.0

    input_timeout5 = ' 00.9900 '
    timeout_expected5 = 0.99

    yield input_timeout1, timeout_expected1, type(timeout_expected1)
    yield input_timeout2, timeout_expected2, type(timeout_expected2)
    yield input_timeout3, timeout_expected3, type(timeout_expected3)
    yield input_timeout4, timeout_expected4, type(timeout_expected4)
    yield input_timeout5, timeout_expected5, type(timeout_expected5)


@pytest.mark.parametrize(
    'input_timeout, timeout_expected, type_expected',
    fixture_correct_timeout()
)
def test_correct_timeout(
    input_timeout: OptionalGrpcTimeout,
    timeout_expected: Optional[float],
    type_expected: type
):
    result_timeout = correct_timeout(input_timeout)

    assert result_timeout == timeout_expected
    assert type(result_timeout) == type_expected


def test_correct_timeout_failed():
    incorrect_value = '.............'

    with pytest.raises(ValueError, match='could not convert string to float:'):
        correct_timeout(incorrect_value)
