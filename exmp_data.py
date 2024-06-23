import pandas as pd
import pyarrow.feather as feather

df = pd.read_feather("processed_data\data_2019-04-01T00_00_00_2019-04-02T00_00_00.feather")
print(df.columns, df.head())
