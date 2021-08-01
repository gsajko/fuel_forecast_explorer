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
# fill missing data between fuel price changes
def pivot_fill(df, sample, to_resample, aggfunc="mean"):
    df.timestamp = pd.to_datetime(df.timestamp)
    df = pd.pivot_table(
        df, values=to_resample, index="timestamp", columns=["siteid"], aggfunc=aggfunc
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

# %%
# def gen_df_brand(df):
#     df_brand = pd.pivot_table(
#         df,
#         values="site_brand",
#         index="timestamp",
#         columns=["siteid"],
#         aggfunc="first",
#     )
#     df_brand.fillna(method="ffill", inplace=True)
#     df_brand.fillna(method="bfill", inplace=True)
#     df_brand = df_brand.stack().reset_index()
#     add_datepart_time(df_brand, "timestamp")
#     df_brand["timestamp"] = pd.to_datetime(
#         df_brand[["year", "month", "day", "hour"]]
#     )
#     df_brand = df_brand.drop(columns=["year", "month", "day", "hour"])
#     df_brand.columns = ["siteid", "brand", "timestamp"]
#     df_brand.drop_duplicates(keep="last", inplace=True)
#     return df_brand


# %%
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
resamp = pivot_fill(to_re, "1D", "price")
resamp.describe()
# %%
print(to_re.siteid.value_counts())
to_re.siteid.value_counts().describe()  # how often does a site change value
# %%
# resamp_df_brand = gen_df_brand(to_re)
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
    df_brand[["year", "month", "day"]]
)
df_brand = df_brand.drop(columns=["year", "month", "day", "hour"])
df_brand.columns = ["siteid", "brand", "timestamp"]
df_brand.drop_duplicates(keep="last", inplace=True)
# %%
# combine dfs
df_brand = df_brand.reset_index(drop=True).groupby(["timestamp", "siteid"]).agg("first")
resamp = resamp.reset_index(drop=True).groupby(["timestamp", "siteid"]).agg("first")

# %%
result = pd.concat([resamp, df_brand], axis=1)
result.dropna(inplace=True)
result = result.reset_index()