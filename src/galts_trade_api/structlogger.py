import logging
import sys
from typing import Dict, Mapping, Optional, TextIO

import structlog
import ujson
from structlog.processors import _figure_out_exc_info

__all__ = ['get_logger', 'configure_json_output', 'UltraJSONRenderer']


def _shy_format_exc_info(_, __, event_dict: Dict) -> Mapping:
    """
    A version of `structlog.processors.format_exc_info` which doesn't dump a tracback.
    Check http://www.structlog.org/en/19.1.0/processors.html for info about processors.
    """
    exc_info = event_dict.pop('exc_info', None)
    if exc_info:
        exc_tuple = _figure_out_exc_info(exc_info)
        event_dict['exceptionClass'] = exc_tuple[0].__name__
        event_dict['exceptionMessage'] = str(exc_tuple[1])
    return event_dict


class UltraJSONRenderer(structlog.processors.JSONRenderer):
    def __init__(self, **dumps_kw):
        self._dumps_kw = dumps_kw
        self._dumps = ujson.dumps


def configure_json_output(enable_unicode_decoder: bool = False, **json_dumps_kwargs) -> None:
    """
    Args:
         json_dumps_kwargs: Optional arguments for JSON serializer. Useful value in development:
            {'indent': 2}
    """
    if structlog.is_configured():
        return

    json_dumps_kwargs.setdefault('escape_forward_slashes', False)

    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper('iso', True),
        structlog.processors.StackInfoRenderer(),
        _shy_format_exc_info,
    ]

    if enable_unicode_decoder:
        processors.append(structlog.processors.UnicodeDecoder())

    processors.append(UltraJSONRenderer(**json_dumps_kwargs))

    structlog.configure_once(
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        # Like http://www.structlog.org/en/19.1.0/performance.html advice
        cache_logger_on_first_use=True
    )


def get_logger(
    name: Optional[str] = None,
    level: Optional[int] = logging.INFO,
    stream: Optional[TextIO] = sys.stdout,
    *args,
    **kwargs
) -> logging.Logger:
    # To correct work of stdlib we should initialize the standard logger with the name before
    # interaction with structlog
    std_logger = logging.getLogger(name)
    if level is not None:
        std_logger.setLevel(level)
    if stream is not None:
        std_logger.addHandler(logging.StreamHandler(stream))

    return structlog.get_logger(name, *args, **kwargs)
