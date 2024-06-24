import asyncio
import time
from binance import get_data
from pd_solution import data_transform_pd
from cycle_solution import data_transform_cycle
import pandas as pd
from pathlib import Path, PurePath

current_file_path = Path(__file__).resolve()
processed_data_dir = PurePath(f"{current_file_path.parent}\processed_data")


def main() -> None:
    iterations = 10
    interval = 60

    solution_pd, solution_cycle = pd.DataFrame(), pd.DataFrame()
    cycle_won = 0

    # starting iterations, for each collecting data and analizing it
    for i in range(iterations):
        print(f"Iteration: {i + 1} from {iterations}")

        # getting data for one timestamp
        timestamp = str(time.time())
        spot_trades, futures_trades, open_interests, funding_rates = asyncio.run(get_data(interval))
        timestamp += " - " + str(time.time())

        # data aggregation using pandas
        start_time = time.time()
        solution = data_transform_pd(spot_trades, futures_trades, open_interests, funding_rates, timestamp)
        end_time_pd = time.time() - start_time
        solution_pd = pd.concat([solution_pd, pd.DataFrame([solution])])

        # data aggregation using cycles
        start_time = time.time()
        solution = data_transform_cycle(spot_trades, futures_trades, open_interests, funding_rates, timestamp)
        end_time_cycle = time.time() - start_time
        solution_cycle = pd.concat([solution_cycle, pd.DataFrame([solution])])

        # counter for winners by time
        if end_time_pd > end_time_cycle:
            cycle_won += 1

        print(f"{len(solution_cycle)} iteration ended")

    # data transformation to csv
    solution_pd.to_csv(f"{processed_data_dir}\solution_pd.csv", index=False)
    solution_cycle.to_csv(f"{processed_data_dir}\solution_cycle.csv", index=False)

    # comparison of the speed (for the 1 minute timestamp speed was approximately the same)
    print(f"#cycle won: {cycle_won}, #pandas won: {len(solution_cycle)-cycle_won}")


if __name__ == "__main__":
    main()
