import asyncio
import datetime
import signal
from typing import Sequence

from galts_trade_api.exchange import DepthConsumeKey
from galts_trade_api.terminal import Terminal
from galts_trade_api.transport.real import RealTransportFactory


async def start_trade_system():
    username = 'vasya'
    password = 'pupkin123'
    symbol_tag = 'BTCUSDT'

    def handle_exception(local_loop: asyncio.AbstractEventLoop, context):
        nonlocal terminal

        if 'terminal' in locals():
            terminal.shutdown_transport()
            del terminal

        print('Caught exception (main process):')
        local_loop.default_exception_handler(context)

        local_loop.create_task(shutdown(local_loop))

    loop = asyncio.get_running_loop()
    loop.set_exception_handler(handle_exception)

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
):
    print(
        f'time={time} exchange_tag={exchange_tag} market_tag={market_tag} symbol_tag={symbol_tag} '
        f'len(bids)={len(bids)} len(asks)={len(asks)}')


async def signal_handler(signal, loop: asyncio.AbstractEventLoop):
    print(f'Received exit signal {signal.name}...')
    await shutdown(loop)


# Inspired by https://www.roguelynn.com/words/asyncio-exception-handling/
async def shutdown(loop: asyncio.AbstractEventLoop):
    """Cleanup tasks tied to the program's shutdown."""
    print('Shutting down...')

    other_tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task(loop)]

    [task.cancel() for task in other_tasks]

    print(f'Cancelling {len(other_tasks)} outstanding tasks')
    await asyncio.gather(*other_tasks, return_exceptions=True)

    loop.stop()


def main():
    loop = asyncio.new_event_loop()
    loop.set_debug(True)

    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(s, lambda s=s: loop.create_task(signal_handler(s, loop)))

    try:
        loop.create_task(start_trade_system())
        loop.run_forever()
    finally:
        # Only with run_forever()
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        print('Successfully shutdown the program.')


if __name__ == '__main__':
    main()
