# %%
import numpy as np
import pandas as pd

# %%
# suppress sci notation
pd.set_option("display.float_format", lambda x: "%.3f" % x)
# %%
# Load data
df = pd.read_csv(
    "../data/aggregated/prices_all_clean.csv.gz",
    compression="gzip",
    index_col=False,
)

df["price"] = df.price.replace(9999, np.NaN)  # replace no_fuel with NaN
df["siteid"] = df["siteid"].astype(str)
df["timestamp"] = pd.to_datetime(df["timestamp"])
df.info(verbose=True, null_counts=True)
# %%

# fill missing data between fuel price changes
def pivot_fill(df, sample, to_resample):
    df.timestamp = pd.to_datetime(df.timestamp)
    df = pd.pivot_table(
        df, values=to_resample, index="timestamp", columns=["siteid"]
    )
    df.fillna(method="ffill", inplace=True)
    df.describe()
    df_resampled = df.resample(sample).mean()
    df_resampled = df_resampled.stack().reset_index()
    df_resampled.columns = ["timestamp", "siteid", to_resample]
    df_resampled.timestamp = pd.to_datetime(df_resampled.timestamp)
    return df_resampled


# %%
def add_datepart_time(df, fldname, drop=True):
    "Helper function that adds columns relevant to a date."
    fld = df[fldname]
    fld_dtype = fld.dtype
    if isinstance(fld_dtype, pd.core.dtypes.dtypes.DatetimeTZDtype):
        fld_dtype = np.datetime64

    if not np.issubdtype(fld_dtype, np.datetime64):
        df[fldname] = fld = pd.to_datetime(fld, infer_datetime_format=True)
    targ_pre = ""
    attr = ["year", "month", "day", "hour"]
    for n in attr:
        df[targ_pre + n] = getattr(fld.dt, n.lower())
    if drop:
        df.drop(fldname, axis=1, inplace=True)


def gen_df_brand(df):
    df_brand = pd.pivot_table(
        df,
        values="site_brand",
        index="timestamp",
        columns=["siteid"],
        aggfunc="first",
    )
    df_brand.fillna(method="ffill", inplace=True)
    df_brand.fillna(method="bfill", inplace=True)
    df_brand = df_brand.stack().reset_index()
    add_datepart_time(df_brand, "timestamp")
    df_brand["timestamp"] = pd.to_datetime(
        df_brand[["year", "month", "day", "hour"]]
    )
    df_brand = df_brand.drop(columns=["year", "month", "day", "hour"])
    df_brand.columns = ["siteid", "brand", "timestamp"]
    df_brand.drop_duplicates(keep="last", inplace=True)
    return df_brand


# %%
to_re = df[df.fuel_type == "e10"]

# %%
resamp = pivot_fill(to_re, "1D", "price")
resamp.describe()
to_re.siteid.value_counts().describe()  # how often does a site change value
# %%
resamp_df_brand = gen_df_brand(to_re)
# %%
df_brand = pd.pivot_table(
    to_re,
    values="site_brand",
    index="timestamp",
    columns=["siteid"],
    aggfunc="first",
)
# %%
df_brand.fillna(method="ffill", inplace=True)
df_brand.fillna(method="bfill", inplace=True)
# %%
df_brand = df_brand.stack().reset_index()
add_datepart_time(df_brand, "timestamp")
# %%
df_brand["timestamp"] = pd.to_datetime(
    df_brand[["year", "month", "day", "hour"]]
)
df_brand = df_brand.drop(columns=["year", "month", "day", "hour"])
df_brand.columns = ["siteid", "brand", "timestamp"]
df_brand.drop_duplicates(keep="last", inplace=True)
# %%
# %%
# %%
# %%


# %%
resamp.describe()
# %%
resamp.set_index("timestamp").resample("1D").mean().plot(
    kind="line", figsize=(16, 6)
)
# %%
resamp[resamp.siteid == "61401007"].set_index("timestamp").plot(
    kind="line", figsize=(16, 6)
)
# %%
fuels_to_resample = ["e10", "unlead", "pulp_9596_ron", "pulp_98_ron"]
