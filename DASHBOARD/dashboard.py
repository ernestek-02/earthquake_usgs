import os
import pandas as pd
import pydeck as pdk
import streamlit as st
from pymongo import MongoClient
from datetime import datetime
from zoneinfo import ZoneInfo

MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = int(os.getenv("MONGO_PORT"))
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

sign = +10000
latest_radius = 30000

client = MongoClient(MONGO_HOST, MONGO_PORT)

db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

st.set_page_config(
    page_title="Terremoti nel mondo",
    layout="wide"
)

st.title("Terremoti nel mondo")

@st.cache_data(ttl=5)
def load_data():
    earthquakes = list(
        collection.find({}, {"_id": 0})
        .sort("time", -1)
        .limit(500)
    )

    total = collection.count_documents({})

    return earthquakes, total

@st.fragment(run_every="1s")
def update_dashboard():
    earthquakes, total = load_data()

    df = pd.DataFrame(earthquakes)

    if df.empty:
        st.warning("Nessun terremoto disponibile.")
        return

    df["time"] = pd.to_datetime(
        df["time"],
        unit="ms",
        utc=True
    ).dt.tz_convert("Europe/Rome")
    
    st.caption(
        f"Ultimo aggiornamento: "
        f"{datetime.now(ZoneInfo('Europe/Rome')).strftime('%H:%M:%S')}"
    )

    st.subheader("Filtri")

    col_filter1, col_filter2 = st.columns(2)

    magnitudo_minima = col_filter1.slider(
        "Magnitudo minima",
        min_value=0.0,
        max_value=10.0,
        value=0.0,
        step=0.1
    )

    periodo = col_filter2.selectbox(
        "Periodo",
        [
            "Tutti",
            "Ultimi 30 minuti",
            "Ultima ora",
            "Ultima 2 ore",
            "Ultime 6 ore",
            "Ultime 12 ore",
            "Ultime 24 ore",   
            "Ultimi 3 giorni",
            "Ultimi 7 giorni",
            "Ultimi 30 giorni"
        ]
    )

    filtered_df = df[
        df["magnitude"] >= magnitudo_minima
    ].copy()

    now = pd.Timestamp.now(tz="Europe/Rome")

    if periodo == "Ultimi 30 minuti":
        filtered_df = filtered_df[
            filtered_df["time"]
            >= now - pd.Timedelta(minutes=30)
        ]

    elif periodo == "Ultima ora":
        filtered_df = filtered_df[
            filtered_df["time"]
            >= now - pd.Timedelta(hours=1)
        ]

    elif periodo == "Ultima 2 ore":
        filtered_df = filtered_df[
            filtered_df["time"]
            >= now - pd.Timedelta(hours=2)
        ]

    elif periodo == "Ultima 6 ore":
        filtered_df = filtered_df[
            filtered_df["time"]
            >= now - pd.Timedelta(hours=6)
        ]

    elif periodo == "Ultima 12 ore":
        filtered_df = filtered_df[
            filtered_df["time"]
            >= now - pd.Timedelta(hours=12)
        ]

    elif periodo == "Ultima 24 ore":  
        filtered_df = filtered_df[
            filtered_df["time"]
            >= now - pd.Timedelta(days=1)
        ]

    elif periodo == "Ultimi 3 giorni":
              
        filtered_df = filtered_df[
            filtered_df["time"]
            >= now - pd.Timedelta(days=3)
        ]

    elif periodo == "Ultimi 7 giorni":     
        filtered_df = filtered_df[
            filtered_df["time"]
            >= now - pd.Timedelta(days=7)
        ]

    elif periodo == "Ultimi 30 giorni":
        filtered_df = filtered_df[
            filtered_df["time"]
            >= now - pd.Timedelta(days=30)
        ]
        
    if filtered_df.empty:
        st.warning(
            "Nessun terremoto corrisponde ai filtri selezionati."
        )
        filtered_df = df.copy()

    magnitudo_media = filtered_df["magnitude"].mean()
    magnitudo_massima = filtered_df["magnitude"].max()
    profondita_media = filtered_df["depth"].mean()
    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Terremoti totali",
        total
    )

    col2.metric(
        "Magnitudo media",
        f"{magnitudo_media:.1f}"
    )

    col3.metric(
        "Magnitudo massima",
        f"{magnitudo_massima:.1f}"
    )

    col4.metric(
        "Profondità media",
        f"{profondita_media:.0f} km"
    )

    st.subheader("Mappa dei terremoti")

    filtered_df["radius"] = (
        filtered_df["magnitude"].clip(lower=0.1) * 15000
    )

    earthquakes_layer = pdk.Layer(
        "ScatterplotLayer",
        data=filtered_df,
        get_position="[longitude, latitude]",
        get_radius="radius",
        get_fill_color=[255, 140, 0, 160],
        pickable=True,
        auto_highlight=True
    )

    ultimo = df.iloc[0]

    ultimo_df = pd.DataFrame(
        [ultimo]
    )

    global latest_radius, sign

    latest_radius += sign

    if latest_radius >= 60000:
        sign = -15000

    if latest_radius <= 30000:
        sign = +15000

    ultimo_df["radius"] = latest_radius

    latest_layer = pdk.Layer(
        "ScatterplotLayer",
        data=ultimo_df,
        get_position="[longitude, latitude]",
        get_radius="radius",
        get_fill_color=[255, 0, 0, 220],
        pickable=True
    )

    view_state = pdk.ViewState(
        latitude=filtered_df["latitude"].mean(),
        longitude=filtered_df["longitude"].mean(),
        zoom=1
    )

    deck = pdk.Deck(
        layers=[
            earthquakes_layer,
            latest_layer
        ],
        initial_view_state=view_state,
        tooltip={
            "html":
                "<b>{place}</b><br/>"
                "Magnitudo: {magnitude}<br/>"
                "Profondità: {depth} km"
        }
    )

    st.pydeck_chart(
        deck,
        width="stretch"
    )

    st.caption(
        "Il punto rosso lampeggiante rappresenta "
        "il terremoto più recente."
    )

    st.subheader("Top 5 terremoti più forti")

    filtered_df["time_display"] = (
        filtered_df["time"]
        .dt.tz_localize(None)
    )

    top5 = (
        filtered_df
        .sort_values("magnitude", ascending=False)
        .head(5)
    )

    st.dataframe(
        top5[
            [
                "place",
                "magnitude",
                "depth",
                "time_display"
            ]
        ].rename(
            columns={
                "place": "Luogo",
                "magnitude": "Magnitudo",
                "depth": "Profondità (km)",
                "time_display": "Data e ora"
            },
        ),
        hide_index=True,
        width="stretch",
        column_config={
            "Luogo": st.column_config.TextColumn(
                "Luogo",
                width="medium"
            ),
            "Magnitudo": st.column_config.NumberColumn(
                "Magnitudo",
                format="%.1f",
                width="strecth",
                alignment="center"
            ),
            "Profondità (km)": st.column_config.NumberColumn(
                "Profondità (km)",
                format="%.0f",
                width="strecth",
                alignment="center"  
            ),
            "Data e ora": st.column_config.DatetimeColumn(
                "Data e ora",
                width="strecth",
                alignment="center"
            )
        }
    )

    st.subheader("Magnitudo dei terremoti")

    st.bar_chart(
        filtered_df["magnitude"]
    )

    st.subheader("Ultimi terremoti")

    filtered_df["latitude_display"] = filtered_df["latitude"].apply(
        lambda x: f"{abs(x):.2f}° {'N' if x >= 0 else 'S'}"
    )

    filtered_df["longitude_display"] = filtered_df["longitude"].apply(
        lambda x: f"{abs(x):.2f}° {'E' if x >= 0 else 'W'}"
    )

    filtered_df["time_display"] = (
        filtered_df["time"]
        .dt.tz_localize(None)
    )

    st.dataframe(
        filtered_df[
            [
                "place",
                "magnitude",
                "depth",
                "latitude_display",
                "longitude_display",
                "time_display"
            ]
        ].rename(
            columns={
                "place": "Luogo",
                "magnitude": "Magnitudo",
                "depth": "Profondità (km)",
                "latitude_display": "Latitudine",
                "longitude_display": "Longitudine",
                "time_display": "Data e ora"
            }
        ),
        hide_index=True,
        column_config={
            "Luogo": st.column_config.TextColumn(
                "Luogo",
                width="medium"
            ),
            "Magnitudo": st.column_config.NumberColumn(
                "Magnitudo",
                format="%.2f",
                width="strecth",
                alignment="center"
            ),
            "Profondità (km)": st.column_config.NumberColumn(
                "Profondità (km)",
                format="%.2f",
                width="strecth",
                alignment="center"
            ),
            "Latitudine": st.column_config.TextColumn(
                "Latitudine",
                width="strecth",
                alignment="center"
            ),
            "Longitudine": st.column_config.TextColumn(
                "Longitudine",
                width="strecth",
                alignment="center"
            ),
            "Data e ora": st.column_config.DatetimeColumn(
                "Data e ora",
                width="strecth",
                alignment="center"
            )
        }
    )

update_dashboard()