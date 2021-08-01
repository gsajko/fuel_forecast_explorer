# %%
import pandas as pd
import numpy as np

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
# functions
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


def pivot_fill(df, sample, col_to_resample):
    df.timestamp = pd.to_datetime(df.timestamp)
    is_float = df[col_to_resample].dtype == np.float64
    if is_float:
        aggfunc = "mean"
    else:
        aggfunc = "first"
    if (not is_float) & (sample != "1D"):
        raise TypeError("Only 1D sample supported for non-float types")
    df = pd.pivot_table(
        df,
        values=col_to_resample,
        index="timestamp",
        columns=["siteid"],
        aggfunc=aggfunc,
    )
    if is_float:
        df.fillna(method="ffill", inplace=True)
        df_resampled = df.resample(sample).mean()
        df_resampled = df_resampled.stack().reset_index()
        df_resampled.columns = ["timestamp", "siteid", col_to_resample]
        df_resampled.timestamp = pd.to_datetime(df_resampled.timestamp)
    else:
        df.fillna(method="ffill", inplace=True)
        df.fillna(method="bfill", inplace=True)
        df_resampled = df.stack().reset_index()
        add_datepart_time(df_resampled, "timestamp")
        df_resampled["timestamp"] = pd.to_datetime(
            df_resampled[["year", "month", "day"]]
        )
        df_resampled = df_resampled.drop(
            columns=["year", "month", "day", "hour"]
        )
        df_resampled.columns = ["siteid", "brand", "timestamp"]
        df_resampled.drop_duplicates(keep="last", inplace=True)

    return df_resampled


# %%
# choose fuel
to_re = df[df.fuel_type == "e10"]
to_re.info(verbose=True, null_counts=True)
# %%
# remove sites that have fewer then 100 entries
s = to_re.siteid.value_counts().reset_index()
s.columns = ["siteid", "counts"]
sites_with_enough_entries = s[s.counts > 100].siteid.tolist()
to_re = to_re[to_re.siteid.isin(sites_with_enough_entries)]
to_re.info(verbose=True, null_counts=True)
# %%
# resample columns to 1D
resamp_price = pivot_fill(to_re, "1D", "price")
resamp_brand = pivot_fill(to_re, "1D", "site_brand")

# concat dfs
resamp_price = (
    resamp_price.reset_index(drop=True)
    .groupby(["timestamp", "siteid"])
    .agg("first")
)
resamp_brand = (
    resamp_brand.reset_index(drop=True)
    .groupby(["timestamp", "siteid"])
    .agg("first")
)
result = pd.concat([resamp_price, resamp_brand], axis=1)
result.dropna(inplace=True)
result = result.reset_index()
# %%
result.to_csv("../data/aggregated/resamp_e10.csv", index=False)

# %%
