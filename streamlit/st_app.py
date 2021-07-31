# %%
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import datetime

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
start_date = st.sidebar.date_input("From: ", datetime.date(2019, 1, 1))
end_date = st.sidebar.date_input("From: ", datetime.date(2021, 5, 1))
fuel_type = st.sidebar.selectbox("fuel type", df_pivot.columns)
resample_type = st.sidebar.selectbox(
    "resample period ", ["1H", "6H", "1D", "3D", "W"]
)
postcodes = st.sidebar.multiselect(
    "Postcode: ", df.site_post_code.unique(), ["4417"]
)

st.sidebar.write(
    "sites in this postcode selling",
    fuel_type,
    "fuel: ",
    df[df.site_post_code.isin(postcodes)].siteid.unique(),
)  # replace with read from file only about sites
sites = st.sidebar.multiselect(
    "Sites: ", df[df.site_post_code.isin(postcodes)].siteid.unique()
)  # replace with read from file only about sites

# plots
filtered_fuel = df_pivot[~df_pivot[fuel_type].isna()]

filtered_fuel.index = pd.to_datetime(filtered_fuel.index)
df_plot = (
    filtered_fuel.loc[start_date:end_date]
    .resample(resample_type)
    .mean()
    .bfill()
)
fig = go.Figure([go.Scatter(x=df_plot.index, y=df_plot[fuel_type])])

st.header("Plot for all sites")
st.plotly_chart(fig, use_container_width=True)


df_pivot_postcodes = pd.pivot_table(
    df[df.site_post_code.isin(postcodes)],
    values="price",
    index="timestamp",
    columns=["fuel_type"],
    aggfunc=np.mean,
)
st.write(
    f"Nr of petrol stations within selected postcodes area selling",
    fuel_type,
    "fuel: ",
)
st.write(df[df.site_post_code.isin(postcodes)].siteid.nunique())
filtered_fuel2 = df_pivot_postcodes[~df_pivot[fuel_type].isna()]
filtered_fuel2.index = pd.to_datetime(filtered_fuel2.index)
df_plot2 = (
    filtered_fuel2.loc[start_date:end_date]
    .resample(resample_type)
    .mean()
    .bfill()
)
fig2 = go.Figure([go.Scatter(x=df_plot2.index, y=df_plot2[fuel_type])])

st.header("Plot for chosen postcodes")
st.plotly_chart(fig2, use_container_width=True)
