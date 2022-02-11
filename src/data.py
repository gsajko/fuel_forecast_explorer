# %%
from pathlib import Path

import numpy as np
import pandas as pd

# %%
RAW_DATA_DIR = "../scrape_data/data/month"
AGG_DATA_DIR = "../data/aggregated"


def price_data_aggregation(RAW_DATA_DIR):
    # list of DataFrames
    # files_list = [path for path in Path(RAW_DATA_DIR).glob("*.csv")]
    raw_inputs = []
    for path in Path(RAW_DATA_DIR).glob("*.csv"):
        file = pd.read_csv(path, encoding="ISO-8859-1")
        raw_inputs.append(file)

    # Some have underscores, some have spaces
    for df in raw_inputs:
        df.columns = [col.replace("ï»¿", "") for col in df.columns]
        df.columns = [col.replace("_", " ") for col in df.columns]
    # combine Dataframes into one
    prices = raw_inputs[0]
    for df in raw_inputs[1:]:
        prices = pd.concat([prices, df])

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
    print(
        "Memory usage of DataFrame: ", prices_df.memory_usage(index=True).sum()
    )
    return prices_df


# %%
price_data_aggregation(RAW_DATA_DIR).to_csv(
    AGG_DATA_DIR + "/prices_all.csv.gz", index=False, compression="gzip"
)

# %%
def data_cleaning():
    df = pd.read_csv(AGG_DATA_DIR + "/prices_all.csv.gz", compression="gzip")
    # Missing values - Brand
    df["site_brand"].fillna(value="Unknown", inplace=True)

    # Data types
    df["siteid"] = df["siteid"].astype(str)
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y%m%d %H:%M")

    # Corr names
    df["site_brand"] = (
        df.site_brand.str.lower().str.replace(" ", "_").str.replace(r"/", "")
    )
    df["fuel_type"] = (
        df.fuel_type.str.lower().str.replace(" ", "_").str.replace("/", "")
    )

    # Cleaning prices
    # >The price of 9999 denotes fuel stock that is temporarily
    # unavailable e.g. tank empty and awaiting new stock.
    # change prices from 9999 to -1
    df["price"] = df.price.replace(9999, -1)
    # remove prices above 3000, and below 600
    df["price"] = np.where(df.price > 3000, np.NaN, df.price)
    df["price"] = df.price.replace(-1, 9999)  # revert back for no fuel to 9999
    df["price"] = np.where(df.price < 600, np.NaN, df.price)
    df = df.dropna()

    # closed sites
    # TODO other solution for closed sites
    closed_sites = pd.read_csv(AGG_DATA_DIR + "/sites_closed.csv", sep=";")
    closed_sites["SiteCode"] = closed_sites["SiteCode"].astype(str)
    closed_sites = closed_sites[
        closed_sites.SiteStatus == "Closed"
    ].SiteCode.tolist()
    df_clean = df[~df.siteid.isin(closed_sites)].copy()
    return df_clean


# %%
# save it
data_cleaning().to_csv(
    AGG_DATA_DIR + "/prices_all_clean.csv.gz",
    index=True,
    compression="gzip",
)

# %%
