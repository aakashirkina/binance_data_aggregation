def params_per_pair_cycle(data: list, name: str):
    """computes the OHLC, volume, buy_volume and trades for the pair"""
    result = {
        f"volume_{name}": 0,
        f"trades_{name}": len(data),
        f"open_{name}": data[0]["p"],
        f"close_{name}": data[-1]["p"],
        f"high_{name}": data[0]["p"],
        f"low_{name}": data[0]["p"],
    }

    # ranges over the data and finds volume, high and low price
    for i in range(len(data)):
        result[f"volume_{name}"] += data[i]["p"] * data[i]["q"]
        if result[f"high_{name}"] < data[i]["p"]:
            result[f"high_{name}"] = data[i]["p"]
        if result[f"low_{name}"] > data[i]["p"]:
            result[f"low_{name}"] = data[i]["p"]

    price_range = result[f"high_{name}"] - result[f"low_{name}"]
    # for short timestamps price don't have enough time to change and we make an exeption
    if price_range == 0:
        result[f"buy_volume_{name}"] = 0
    else:
        result[f"buy_volume_{name}"] = result[f"volume_{name}"] * (result[f"close_{name}"] - result[f"low_{name}"]) / price_range

    return result


def Mean(data: list, param: str) -> float:
    result = 0
    for i in range(len(data)):
        result += data[i][param]
    return result / len(data)


def Mean_weighted(data1: list, param1: str, data2: list, param2: str) -> float:
    result = 0
    for i in range(len(data1)):
        result += data1[i][param1] * data2[i][param2]
    return result / len(data1)


def data_transform_cycle(
    spots: list,
    perpetual: list,
    open_int: list,
    fund_rate: list,
    timestamp: str,
):
    """aggregates the data we got from binance"""
    new_row = {"timestamp": timestamp}
    new_row = {**new_row, **params_per_pair_cycle(spots, "binance_btcusdt")}
    new_row = {
        **new_row,
        **params_per_pair_cycle(perpetual, "binance_btcusdt_perpetual"),
    }
    new_row["trades_total"] = new_row["trades_binance_btcusdt"] + new_row["trades_binance_btcusdt_perpetual"]
    new_row["volume_total"] = new_row["volume_binance_btcusdt"] + new_row["volume_binance_btcusdt_perpetual"]
    new_row["buy_volume_total"] = new_row["buy_volume_binance_btcusdt"] + new_row["buy_volume_binance_btcusdt_perpetual"]
    new_row["oi"] = Mean(open_int, "openInterest")
    new_row["fund_rate"] = Mean(fund_rate, "fundingRate")
    new_row["funding_rate_oi_weighted"] = Mean_weighted(open_int, "openInterest", fund_rate, "fundingRate")
    return new_row
