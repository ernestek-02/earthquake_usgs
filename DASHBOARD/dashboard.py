import os
import pandas as pd
import pydeck as pdk
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, max
from datetime import datetime
from zoneinfo import ZoneInfo

st.set_page_config(
    page_title="Terremoti nel mondo",
    layout="wide"
)

MONGO_HOSTS = os.getenv("MONGO_HOSTS")
MONGO_REPLICA_SET = os.getenv("MONGO_REPLICA_SET")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

MONGO_URI = (
    f"mongodb://{MONGO_HOSTS}/"
    f"{MONGO_DB}.{MONGO_COLLECTION}"
    f"?replicaSet={MONGO_REPLICA_SET}"
)

sign = +10000
latest_radius = 30000

@st.cache_resource
def create_spark_session():
    return (
        SparkSession.builder
        .appName("EarthquakeDashboard")
        .master("spark://spark-master:7077")
        .config("spark.cores.max", "4")
        .config("spark.executor.cores", "4")
        .config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0"
        )
        .config(
            "spark.mongodb.read.connection.uri",
            MONGO_URI
        )
        .config(
            "spark.driver.bindAddress",
            "0.0.0.0"
        )
        .config(
            "spark.driver.host",
            "dashboard"
        )
        .config(
            "spark.driver.port",
            "39001"
        )
        .getOrCreate()
    )

spark = create_spark_session()

st.title("Terremoti nel mondo")

@st.cache_data(ttl=5)
def load_data(magnitudo_minima, periodo):

    spark_df = (
        spark.read
        .format("mongodb")
        .option(
            "connection.uri",
            MONGO_URI
        )
        .load()
    )

    required_columns = [
        "id",
        "magnitude",
        "place",
        "time",
        "longitude",
        "latitude",
        "depth"
    ]

    if not all(
        column in spark_df.columns
        for column in required_columns
    ):
        return None, 0, None, None, 0

    spark_df = spark_df.select(
        "id",
        "magnitude",
        "place",
        "time",
        "longitude",
        "latitude",
        "depth"
    )

    total = spark_df.count()

    filtered_spark_df = spark_df.filter(
        col("magnitude") >= magnitudo_minima
    )

    now_ms = int(
        datetime.now(
            ZoneInfo("Europe/Rome")
        ).timestamp() * 1000
    )

    periodo_ms = {
        "Ultimi 30 minuti": 30 * 60 * 1000,
        "Ultima ora": 60 * 60 * 1000,
        "Ultime 2 ore": 2 * 60 * 60 * 1000,
        "Ultime 6 ore": 6 * 60 * 60 * 1000,
        "Ultime 12 ore": 12 * 60 * 60 * 1000,
        "Ultime 24 ore": 24 * 60 * 60 * 1000,
        "Ultimi 3 giorni": 3 * 24 * 60 * 60 * 1000,
        "Ultimi 7 giorni": 7 * 24 * 60 * 60 * 1000,
        "Ultimi 30 giorni": 30 * 24 * 60 * 60 * 1000
    }

    if periodo != "Tutti":
        filtered_spark_df = filtered_spark_df.filter(
            col("time") >= now_ms - periodo_ms[periodo]
        )

    filtered_spark_df = filtered_spark_df.cache()

    filtered_total = filtered_spark_df.count()

    if filtered_total == 0:
        filtered_spark_df.unpersist()
        return None, total, None, None, 0

    stats = (
        filtered_spark_df
        .agg(
            avg("magnitude").alias("magnitudo_media"),
            max("magnitude").alias("magnitudo_massima"),
            avg("depth").alias("profondita_media")
        )
        .collect()[0]
        .asDict()
    )

    earthquakes = (
        filtered_spark_df
        .orderBy(
            col("time").desc()
        )
        .limit(500)
        .toPandas()
    )

    top5 = (
        filtered_spark_df
        .orderBy(
            col("magnitude").desc()
        )
        .limit(5)
        .toPandas()
    )

    filtered_spark_df.unpersist()

    return (
        earthquakes,
        total,
        stats,
        top5,
        filtered_total
    )

@st.fragment(run_every="5s")
def update_dashboard():
    
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
            "Ultime 2 ore",
            "Ultime 6 ore",
            "Ultime 12 ore",
            "Ultime 24 ore",   
            "Ultimi 3 giorni",
            "Ultimi 7 giorni",
            "Ultimi 30 giorni"
        ]
    )

    earthquakes, total, spark_stats, top5, filtered_total = load_data(
        magnitudo_minima,
        periodo
    )

    if earthquakes is None:
        st.warning(
            "Nessun terremoto corrisponde ai filtri selezionati."
        )
        return

    df = earthquakes.copy()

    df["time"] = pd.to_datetime(
        df["time"],
        unit="ms",
        utc=True
    ).dt.tz_convert("Europe/Rome")

    top5["time"] = pd.to_datetime(
        top5["time"],
        unit="ms",
        utc=True
    ).dt.tz_convert("Europe/Rome")

    filtered_df = df

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Terremoti totali",
        filtered_total
    )

    col2.metric(
        "Magnitudo media",
        f"{spark_stats['magnitudo_media']:.1f}"
    )

    col3.metric(
        "Magnitudo massima",
        f"{spark_stats['magnitudo_massima']:.1f}"
    )

    col4.metric(
        "Profondità media",
        f"{spark_stats['profondita_media']:.0f} km"
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

    ultimo = filtered_df.iloc[0]

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

    top5["time_display"] = (
        top5["time"]
        .dt.tz_localize(None)
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
                width="stretch",
                alignment="center"
            ),
            "Profondità (km)": st.column_config.NumberColumn(
                "Profondità (km)",
                format="%.0f",
                width="stretch",
                alignment="center"  
            ),
            "Data e ora": st.column_config.DatetimeColumn(
                "Data e ora",
                width="stretch",
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
                width="stretch",
                alignment="center"
            ),
            "Profondità (km)": st.column_config.NumberColumn(
                "Profondità (km)",
                format="%.2f",
                width="stretch",
                alignment="center"
            ),
            "Latitudine": st.column_config.TextColumn(
                "Latitudine",
                width="stretch",
                alignment="center"
            ),
            "Longitudine": st.column_config.TextColumn(
                "Longitudine",
                width="stretch",
                alignment="center"
            ),
            "Data e ora": st.column_config.DatetimeColumn(
                "Data e ora",
                width="stretch",
                alignment="center"
            )
        }
    )

update_dashboard()