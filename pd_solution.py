import pandas as pd


def params_per_pair_pd(data: pd.DataFrame, name: str) -> dict:
    """computes the OHLC, volume, buy_volume and trades for the pair"""
    result = dict()
    (
        result[f"open_{name}"],
        result[f"high_{name}"],
        result[f"low_{name}"],
        result[f"close_{name}"],
    ) = (data.iloc[0]["p"], data["p"].max(), data["p"].min(), data.iloc[-1]["p"])

    result[f"volume_{name}"] = (data["p"] * data["q"]).sum()

    price_range = result[f"high_{name}"] - result[f"low_{name}"]
    # for short timestamps price don't have enough time to change and we make an exeption
    if price_range == 0:
        result[f"buy_volume_{name}"] = 0
    else:
        result[f"buy_volume_{name}"] = result[f"volume_{name}"] * (result[f"close_{name}"] - result[f"low_{name}"]) / price_range

    result[f"trades_{name}"] = len(data)
    return result


def data_transform_pd(
    spots: list,
    perpetual: list,
    open_int: list,
    fund_rate: list,
    timestamp: str,
) -> dict:
    """aggregates the data we got from binance"""
    spots_df, perpetual_df, open_int_df, fund_rate_df = (
        pd.DataFrame(spots),
        pd.DataFrame(perpetual),
        pd.DataFrame(open_int),
        pd.DataFrame(fund_rate),
    )

    new_row = {"timestamp": timestamp}
    new_row = {**new_row, **params_per_pair_pd(spots_df, "binance_btcusdt")}
    new_row = {
        **new_row,
        **params_per_pair_pd(perpetual_df, "binance_btcusdt_perpetual"),
    }
    new_row["trades_total"] = new_row["trades_binance_btcusdt"] + new_row["trades_binance_btcusdt_perpetual"]
    new_row["volume_total"] = new_row["volume_binance_btcusdt"] + new_row["volume_binance_btcusdt_perpetual"]
    new_row["buy_volume_total"] = new_row["buy_volume_binance_btcusdt"] + new_row["buy_volume_binance_btcusdt_perpetual"]
    new_row["oi"] = open_int_df["openInterest"].mean()
    new_row["fund_rate"] = fund_rate_df["fundingRate"].mean()
    new_row["funding_rate_oi_weighted"] = (open_int_df["openInterest"] * fund_rate_df["fundingRate"]).mean()
    return new_row
