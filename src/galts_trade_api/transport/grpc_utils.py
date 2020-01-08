import uuid
from datetime import timezone
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Union

from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message
from google.protobuf.pyext._message import MessageMapContainer, RepeatedCompositeContainer, \
    RepeatedScalarContainer
from google.protobuf.timestamp_pb2 import Timestamp


def generate_request_id() -> str:
    return uuid.uuid4().hex


def get_metadata(request_id: Optional[str] = None) -> List:
    return [('request_id', request_id if request_id is not None else generate_request_id())]


def fill_flat_message(message: Message, entity: Mapping) -> None:
    for name, value in entity.items():
        if value is None:
            message.ClearField(name)
            continue

        message_attr = getattr(message, name)

        if isinstance(message_attr, Timestamp):
            message_attr.FromDatetime(value)
        else:
            setattr(message, name, value)


def message_to_dict(
    msg: Message,
    set_default_values: bool = True,
    use_integers_for_enums: bool = True,
    convert_datetime_to_utc: bool = True
) -> Dict[str, Any]:
    """
    The function is not tested to work with fields of google.protobuf.Struct etc.
    Function converts 'amount' field_descriptor from string to Decimal and Timestamp fields
    to datetime.
    """

    def pythonify_value(field_name: str, field_value) -> Any:
        if isinstance(field_value, Timestamp):
            dt = field_value.ToDatetime()
            if convert_datetime_to_utc:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        if isinstance(field_value, str) and field_name == 'amount':
            return Decimal(field_value)

        return field_value

    def is_map_type_field(field_descriptor: FieldDescriptor) -> bool:
        if field_descriptor.label != FieldDescriptor.LABEL_REPEATED:
            return False
        if field_descriptor.message_type is None:
            return False
        if not field_descriptor.message_type.GetOptions().map_entry:
            return False

        return True

    def is_enum_type_field(field_descriptor: FieldDescriptor) -> bool:
        return field_descriptor.type == field_descriptor.TYPE_ENUM

    def get_enum_value_name(field_name: str, field_value: Any, local_msg: Message) -> str:
        return local_msg.DESCRIPTOR.fields_by_name[field_name].enum_type. \
            values_by_number.get(field_value).name

    def get_actual_field_value(field_name: str, local_msg: Message) -> Any:
        for field_descriptor, field_value in local_msg.ListFields():
            if field_descriptor.name == field_name:
                if isinstance(field_value, RepeatedCompositeContainer):
                    return [convert_descriptor(f) for f in field_value]
                if isinstance(field_value, RepeatedScalarContainer):
                    if is_enum_type_field(field_descriptor) and not use_integers_for_enums:
                        return [get_enum_value_name(field_name, n, local_msg) for n in field_value]
                    return list(field_value)
                if isinstance(field_value, MessageMapContainer):
                    return {k: convert_descriptor(field_value[k]) for k in field_value}
                if is_enum_type_field(field_descriptor) and not use_integers_for_enums:
                    return get_enum_value_name(field_name, field_value, local_msg)

                return pythonify_value(field_name, field_value)

        return None

    def convert_descriptor(local_msg: Message) -> Dict[str, Any]:
        result = {}
        if len(local_msg.DESCRIPTOR.oneofs_by_name.items()):
            result['which_one_of'] = {}
            for k, v in local_msg.DESCRIPTOR.oneofs_by_name.items():
                result['which_one_of'][k] = local_msg.WhichOneof(k)

        for field_name, field_descriptor in local_msg.DESCRIPTOR.fields_by_name.items():
            value = get_actual_field_value(field_name, local_msg)

            if value is None:
                if not set_default_values:
                    continue

                # Any composite type has a default value as empty list,
                # but we want a dict for the map
                if is_map_type_field(field_descriptor):
                    value = {}
                else:
                    value = field_descriptor.default_value

                # for case if enum value has been set to 0 (default value) or not specified
                if is_enum_type_field(field_descriptor) and not use_integers_for_enums:
                    value = get_enum_value_name(field_name, value, local_msg)

            result[field_name] = value

        return result

    return convert_descriptor(msg)


OptionalGrpcTimeout = Optional[Union[str, int, float]]


def correct_timeout(timeout: OptionalGrpcTimeout = None) -> Optional[float]:
    if timeout is not None:
        timeout = float(timeout)
        if timeout <= 0:
            timeout = None

    return timeout
