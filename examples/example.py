import datetime

from galts_trade_api.asyncio_helper import AsyncProgramEnv, run_program_forever
from galts_trade_api.structlogger import get_logger
from galts_trade_api.terminal import DepthConsumeKey, PriceDepth, Terminal
from galts_trade_api.transport.real import RealTransportFactory

logger = get_logger('ts')


async def start_trade_system(program_env: AsyncProgramEnv) -> None:
    username = 'vasya'
    password = 'pupkin123'

    transport = RealTransportFactory(
        exchange_info_dsn='exchange-info.zone:50051',
        depth_scraping_queue_dsn='amqp://depth-scraping.zone/%2F?heartbeat_interval=10',
        depth_scraping_queue_exchange='depth_updates',
    )
    logger.debug(f'transport={transport}')

    terminal = Terminal.factory(transport)
    logger.debug(f'terminal={terminal}')
    await terminal.init_transport()

    if not await terminal.auth_user(username, password):
        raise RuntimeError(f'Cannot auth {username}')

    await terminal.init_exchange_entities()
    await terminal.wait_exchange_entities_inited()

    await terminal.subscribe_to_prices(
        on_price,
        {
            DepthConsumeKey(exchange_tag='binance', symbol_tag='BTCUSDT'),
            DepthConsumeKey(exchange_tag='binance', symbol_tag='BTCUSDC'),
        }
    )

    logger.info('Init finished!')


async def on_price(
    exchange_tag: str,
    market_tag: str,
    symbol_tag: str,
    time: datetime.datetime,
    bids: PriceDepth,
    asks: PriceDepth
) -> None:
    logger.info(
        'on_price',
        time=str(time),
        exchange_tag=exchange_tag,
        market_tag=market_tag,
        symbol_tag=symbol_tag,
        bids=len(bids),
        asks=len(asks)
    )


if __name__ == '__main__':
    run_program_forever(start_trade_system)
