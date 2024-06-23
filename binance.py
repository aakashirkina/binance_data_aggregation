import json
import time
import asyncio
import requests
import websockets


async def get_data(duration: int) -> tuple[list, list, list, list]:
    """connects to binance and gets the data about pairs BTC-USDT (spot Binance) and BTC-USDT (perp Binance-futures)"""
    uri_spot = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    uri_futures = "wss://fstream.binance.com/ws/btcusdt@trade"

    # we store each response in lists, because we don't use pandas in the second solution
    spot_trades = []
    futures_trades = []
    open_interests = []
    funding_rates = []

    end_time = time.time() + duration

    # want to be sure that each trade is written only once
    unique_spot_trades = set()
    unique_futures_trades = set()

    async with websockets.connect(uri_spot, ping_timeout=30, ping_interval=30) as websocket_spot, websockets.connect(
        uri_futures, ping_interval=30, ping_timeout=30
    ) as websocket_futures:
        while time.time() < end_time:
            spot_response = await websocket_spot.recv()
            futures_response = await websocket_futures.recv()

            # getting the response
            spot_trade = json.loads(spot_response)
            futures_trade = json.loads(futures_response)

            # adding only those params that we are going to use ('q' for quantity, 'p' for price)
            if spot_trade["t"] not in unique_spot_trades:
                unique_spot_trades.add(spot_trade["t"])
                spot_trades.append({"p": float(spot_trade["p"]), "q": float(spot_trade["q"])})

            if futures_trade["t"] not in unique_futures_trades:
                unique_futures_trades.add(futures_trade["t"])
                futures_trades.append({"p": float(futures_trade["p"]), "q": float(futures_trade["q"])})

            # getting additional data
            open_interest = requests.get(
                "https://fapi.binance.com/fapi/v1/openInterest",
                params={"symbol": "BTCUSDT"},
            ).json()
            funding_rate = requests.get(
                "https://fapi.binance.com/fapi/v1/fundingRate",
                params={"symbol": "BTCUSDT", "limit": 1},
            ).json()[0]

            open_interests.append({"openInterest": float(open_interest["openInterest"])})
            funding_rates.append({"fundingRate": float(funding_rate["fundingRate"])})

        await asyncio.sleep(1)

    return spot_trades, futures_trades, open_interests, funding_rates
