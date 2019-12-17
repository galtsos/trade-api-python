import asyncio
import datetime
import sys
from typing import Sequence

from galts_trade_api.exchange import DepthConsumeKey
from galts_trade_api.terminal import Terminal
from galts_trade_api.transport.real import RealTransportFactory


async def main():
    username = 'vasya'
    password = 'pupkin123'
    symbol_tag = 'BTCUSDT'

    def on_exception(e: BaseException):
        nonlocal terminal

        if 'terminal' in locals():
            terminal.shutdown_transport()
            del terminal

        print('An exception has been occur in the processes')
        print(repr(e))
        sys.exit(0)

    try:
        transport = RealTransportFactory()
        transport.configure_endpoints(
            exchange_info_dsn='exchange-info.zone:50051',
            depth_scraping_queue_dsn='amqp://depth-scraping.zone/%2F?heartbeat_interval=10',
            depth_scraping_queue_exchange='depth_updates',
        )
        print(f'transport={transport}')

        terminal = Terminal(transport, on_exception)
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

        while True:
            await asyncio.sleep(1)
    except Exception as e:
        on_exception(e)


async def on_price(
    exchange_tag: str,
    market_tag: str,
    symbol_tag: str,
    time: datetime.datetime,
    bids: Sequence,
    asks: Sequence
):
    print(
        f'time={time} exchange_tag={exchange_tag} market_tag={market_tag} symbol_tag={symbol_tag} '
        f'len(bids)={len(bids)} len(asks)={len(asks)}')


if __name__ == '__main__':
    asyncio.run(main())
