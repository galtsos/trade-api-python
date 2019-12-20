import asyncio
import datetime
from typing import Dict, Sequence

from galts_trade_api.asyncio_helper import AsyncProgramEnv, run_program_forever
from galts_trade_api.exchange import DepthConsumeKey
from galts_trade_api.terminal import Terminal
from galts_trade_api.transport.real import RealTransportFactory


async def start_trade_system(program_env: AsyncProgramEnv) -> None:
    username = 'vasya'
    password = 'pupkin123'
    symbol_tag = 'BTCUSDT'

    def exception_handler(local_loop: asyncio.AbstractEventLoop, context: Dict) -> None:
        nonlocal terminal

        if 'terminal' in locals():
            terminal.shutdown_transport()
            del terminal

    program_env.exception_handler_patch = exception_handler

    transport = RealTransportFactory()
    transport.configure_endpoints(
        exchange_info_dsn='exchange-info.zone:50051',
        depth_scraping_queue_dsn='amqp://depth-scraping.zone/%2F?heartbeat_interval=10',
        depth_scraping_queue_exchange='depth_updates',
    )
    print(f'transport={transport}')

    terminal = Terminal(transport)
    print(f'terminal={terminal}')
    await terminal.init_transport()

    if not await terminal.auth_user(username, password):
        raise RuntimeError(f'Cannot auth {username}')

    await terminal.init_exchange_entities()
    await terminal.wait_exchange_entities_inited()

    binance = terminal.get_exchange('binance')
    print(f'binance={binance}')

    market = binance.get_market_by_custom_tag(symbol_tag)
    print(f'market={market}')
    await market.subscribe_to_prices(
        on_price,
        [
            # DepthConsumeKey(symbol_tag='BNBBTC'),
            # DepthConsumeKey(symbol_tag='BTCUSDC'),
        ]
    )

    print('Init finished!')


async def on_price(
    exchange_tag: str,
    market_tag: str,
    symbol_tag: str,
    time: datetime.datetime,
    bids: Sequence,
    asks: Sequence
) -> None:
    print(
        f'time={time} exchange_tag={exchange_tag} market_tag={market_tag} symbol_tag={symbol_tag} '
        f'len(bids)={len(bids)} len(asks)={len(asks)}')


if __name__ == '__main__':
    run_program_forever(start_trade_system)
