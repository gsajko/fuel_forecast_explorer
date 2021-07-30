# %%
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

# %%
df = pd.read_csv("data/aggregated/prices_all.csv.gz", compression="gzip")
df.isna().sum()
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
filtered_fuel = df_prices[fuel_type][
    ~df_prices[fuel_type].isna()
]

filtered_fuel.index = pd.to_datetime(filtered_fuel.index)
fig = go.Figure(
    [
        go.Scatter(
            x=filtered_fuel.index,
            y=filtered_fuel.resample("3D").mean().bfill(),
        )
    ]
)

st.plotly_chart(fig, use_container_width=True)
