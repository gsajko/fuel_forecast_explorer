# %%
from pathlib import Path

import pandas as pd

# %%
RAW_DATA_DIR = "../scrape_data/data/month"
AGG_DATA_DIR = "aggregated"

# %%
raw_inputs = []
for path in Path(RAW_DATA_DIR).glob("*.csv"):
    file = pd.read_csv(path, encoding="ISO-8859-1")
    raw_inputs.append(file)
print(len(raw_inputs))
# %%
# Some have underscores, some have spaces
for df in raw_inputs:
    df.columns = [col.replace("ï»¿", "") for col in df.columns]
    df.columns = [col.replace("_", " ") for col in df.columns]
# combine Dataframes into one
# %%
prices = raw_inputs[0]

# %%

for df in raw_inputs[1:]:
    prices = pd.concat([prices, df])
# %%
prices["timestamp"] = pd.to_datetime(
    prices["TransactionDateutc"], format="%d/%m/%Y %H:%M"
)
prices.SiteId = prices.SiteId.astype("str")
prices.columns = [x.lower() for x in prices.columns]
prices.columns = [x.replace(" ", "_") for x in prices.columns]
prices_df = pd.DataFrame(
    prices,
    columns=[
        "siteid",
        "site_brand",
        "fuel_type",
        "site_post_code",
        "timestamp",
        "price",
    ],
)
# %%
print("Memory usage of DataFrame: ", prices_df.memory_usage(index=True).sum())
# %%
prices_df.to_csv(
    AGG_DATA_DIR + "/prices_all.csv.gz", index=False, compression="gzip"
)
# %%
prices_df.to_csv(AGG_DATA_DIR + "/prices_all.csv", index=False)
# %%
