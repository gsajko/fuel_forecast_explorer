# %%
import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go

import streamlit as st

# %%
if "df_prices_clean" not in st.session_state:
    df = pd.read_csv(
        "data/aggregated/prices_all_clean.csv.gz", compression="gzip"
    )
    df["price"] = df.price.replace(9999, np.NaN)
    df["site_post_code"] = df["site_post_code"].astype("str")
    df["siteid"] = df["siteid"].astype("str")
    df.isna().sum()
    df = df.dropna()
    st.session_state.df_prices_clean = df
    print("getting df prices")


df = st.session_state.df_prices_clean
# %%
if "df_pivot" not in st.session_state:
    df_prices = pd.pivot_table(
        df,
        values="price",
        index="timestamp",
        columns=["fuel_type"],
        aggfunc=np.mean,
    )
    st.session_state.df_pivot = df_prices
df_pivot = st.session_state.df_pivot
# %%
# sidebar
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
start_date = st.sidebar.date_input("From: ", datetime.date(2021, 1, 1))
end_date = st.sidebar.date_input("From: ", yesterday)
fuel_type = st.sidebar.selectbox(
    "fuel type", df.fuel_type.value_counts().index.to_list(), 1
)
# second most freq sold fuel type as default
resample_type = st.sidebar.selectbox(
    "resample period ", ["1H", "6H", "1D", "3D", "W"], 2
)
postcodes = st.sidebar.multiselect(
    "Postcode: ", df.site_post_code.unique(), ["4209"]
)
# TODO not all postcodes are displayed? no 4000


dfx = df[
    (df.site_post_code.isin(postcodes)) & (df.fuel_type == fuel_type)
].sort_values("timestamp")
dfx.index = pd.to_datetime(dfx.timestamp)
dfx.drop(labels="timestamp", axis=1, inplace=True)
st.write(
    "Nr of petrol stations within selected postcodes area selling",
    fuel_type,
    "fuel: ",
    dfx.siteid.nunique(),
)

dfx_prices = pd.pivot_table(
    dfx.loc[start_date:end_date],
    values="price",
    index="timestamp",
    columns=["siteid"],
    aggfunc=np.mean,
)

df_plot_sites = dfx_prices.resample(resample_type).mean().bfill()  # fill nans

sites = st.sidebar.multiselect(
    "Chose petrol station: ", dfx_prices.columns
)  # replace with read from file only about sites
# TODO add here timestamp --


# plots
filtered_fuel = df_pivot[~df_pivot[fuel_type].isna()]

filtered_fuel.index = pd.to_datetime(filtered_fuel.index)
df_plot = (
    filtered_fuel.loc[start_date:end_date]
    .resample(resample_type)
    .mean()
    .bfill()
)

df_plot_postcode = (
    dfx_prices.loc[start_date:end_date].resample(resample_type).bfill()
)

QLD_mean_plot = st.sidebar.checkbox("show QLD mean on the plot", value=True)
postcode_mean_plot = st.sidebar.checkbox("show mean based on postcodes")

fig2 = go.Figure()
if QLD_mean_plot:
    fig2.add_trace(
        go.Scatter(
            x=df_plot.index,
            y=df_plot[fuel_type],
            name="QLD mean",
            line=dict(
                color="rgba(160,210,210, 0.3)",
                width=6,
            ),
        )
    )
if postcode_mean_plot:
    fig2.add_trace(
        go.Scatter(
            x=df_plot_postcode.index,
            y=df_plot_postcode.mean(axis=1),
            name="postcode mean",
            line=dict(
                color="rgba(160,160,160, 0.2)",
                width=8,
            ),
        )
    )

for site in sites:
    fig2.add_trace(
        go.Scatter(x=df_plot_sites.index, y=df_plot_sites[site], name=site)
    )
st.header("Plot for all chosen sites")
st.plotly_chart(fig2, use_container_width=True)


# TODO
# add plot for chosen postcodes
# with select option - for displying "QLD mean", "Postcode MEAN"
