# %%
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

# %%
df = pd.read_csv("data/aggregated/prices_all_clean.csv.gz", compression="gzip")
df["price"] = df.price.replace(9999, np.NaN)
df.isna().sum()
df = df.dropna()
st.write(df.shape)
# %%
df_prices = pd.pivot_table(
    df,
    values="price",
    index="timestamp",
    columns=["fuel_type"],
    aggfunc=np.mean,
)
# %%
st.write(df_prices.shape)

# %%
fuel_type = st.sidebar.selectbox("fuel type", df_prices.columns)
resample_type = st.sidebar.selectbox("resample period ", ["1D", "3D", "W"])
filtered_fuel = df_prices[
    ~df_prices[fuel_type].isna()
]

filtered_fuel.index = pd.to_datetime(filtered_fuel.index)
df_plot = filtered_fuel.resample(resample_type).mean().bfill()
fig = go.Figure(
    [
        go.Scatter(
            x=df_plot.index,
            y=df_plot[fuel_type],
            line={'smoothing': 1.3}
        )
    ]
)

st.plotly_chart(fig, use_container_width=True)
st.write(df_plot.shape)

