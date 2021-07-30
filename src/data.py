# %%
import pandas as pd
from pathlib import Path

# %%
RAW_DATA_DIR = "data/raw"
AGG_DATA_DIR = "data/aggregated"

def price_data_aggregation(RAW_DATA_DIR):
    # list of DataFrames
    # files_list = [path for path in Path(RAW_DATA_DIR).glob("*.csv")]
    raw_inputs = []
    for path in Path(RAW_DATA_DIR).glob("*.csv"):
        file = pd.read_csv(path, encoding = "ISO-8859-1")
        raw_inputs.append(file) 
        
    # Some have underscores, some have spaces
    for df in raw_inputs:
        df.columns = [col.replace("_", " ") for col in df.columns]
    # combine Dataframes into one
    prices = raw_inputs[0]
    for df in raw_inputs[1:]:
        prices = pd.concat([prices, df])

    prices["timestamp"] = pd.to_datetime(
        prices["TransactionDateutc"], format="%d/%m/%Y %H:%M"
    )
    prices.columns = [x.lower() for x in prices.columns]
    prices.columns = [x.replace(" ", "_") for x in prices.columns]
    prices_df = pd.DataFrame(
        prices, columns=["siteid", "site_brand", "fuel_type", "timestamp", "price"]
    )
    print("Memory usage of DataFrame: ", prices_df.memory_usage(index=True).sum())
    return prices_df
# %%
price_data_aggregation(RAW_DATA_DIR).to_csv(AGG_DATA_DIR + "/prices_all.csv.gz", index=False, compression="gzip")


