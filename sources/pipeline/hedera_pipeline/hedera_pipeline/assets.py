import base64
from datetime import datetime, timedelta
from typing import Union
import json

from dagster import asset, MaterializeResult, MetadataValue
from dagster_duckdb import DuckDBResource
from dotenv import find_dotenv, dotenv_values
import pymongo
import numpy as np
np.float_ = np.float64

from prophet import Prophet
from prophet.plot import plot_plotly
import plotly.io as pio

from .utils import get_transactions
from .constants import (
    CRYPTO_TRANSFER_INITIAL_STORAGE_FILE,
    CRYPTO_TRANSFER_TABLE_NAME,
    CRYPTO_TRANSFER_PREDICTION_STORAGE_FILE,
    CRYPTO_TRANSFER_PREDICTION_PLOT_FILE,
    ATLAS_URI
)


transaction_types = ["CONTRACTCALL", "CRYPTOTRANSFER"]


@asset(
    group_name="raw_data",
    compute_kind="Python"
    )
def crypto_transfer_transactions():
    """ Get the transactions data and store it in a json file """

    end_time = datetime.now()
    start_time = (end_time - timedelta(seconds=3600.0)).timestamp()

    transactions: list[dict | None] = get_transactions(start_time, 100, transaction_types[1], "asc")
    
    with open(CRYPTO_TRANSFER_INITIAL_STORAGE_FILE, "w") as f:
        f.write(json.dumps(transactions))

    return MaterializeResult(
        metadata={
            "Number of records": len(transactions),
            "Records start time": transactions[0]["time_tx_occured"],
            "Records end time": transactions[-1]["time_tx_occured"]
        }
    )


@asset(
    group_name="ingested",
    compute_kind="DuckDB",
    deps=["crypto_transfer_transactions"]
)
def db_crypto_transfer_transactions(database: DuckDBResource):
    """ Ingest the transactions data into the Database """

    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS '{CRYPTO_TRANSFER_TABLE_NAME}' (
            -- y is the amount charged for the transaction
            -- ds is the time the transaction took place

            y DECIMAL,
            ds DATETIME UNIQUE
        )
    """

    ingest_data_query = f"""
        INSERT INTO '{CRYPTO_TRANSFER_TABLE_NAME}' (
            SELECT * FROM read_json_auto('{CRYPTO_TRANSFER_INITIAL_STORAGE_FILE}')
        )
    """

    with database.get_connection() as conn:
        conn.execute(create_table_query)
        try:
            conn.execute(ingest_data_query)
        except Exception:
            pass
        db_rows = conn.sql(f"""
            SELECT COUNT(*) FROM '{CRYPTO_TRANSFER_TABLE_NAME}'
        """).fetchall()[0][0]

    return MaterializeResult(
        metadata={
            "Number of rows in DB": db_rows,
        }
    )


@asset(
    group_name="predict",
    compute_kind="Python",
    deps=["db_crypto_transfer_transactions"]
)
def predict_data(database: DuckDBResource):
    """ Predict future transactions """

    query = f"""
        SELECT * FROM '{CRYPTO_TRANSFER_TABLE_NAME}'
    """

    with database.get_connection() as conn:
        df = conn.execute(query).fetch_df()

    m = Prophet()
    m.fit(df)
    future = m.make_future_dataframe(periods=60, freq="min")
    forecast = m.predict(future)
    forecast_dict = forecast[["ds", "yhat"]].to_dict()
    new_data = {"x": [], "y": []}

    ds = forecast_dict["ds"]
    y = forecast_dict["yhat"]
    for value in list(ds.values())[-60:]:
        new_data["x"].append(str(value))
    for value in list(y.values())[-60:]:
        new_data["y"].append(value)

    t_from = str(datetime.fromisoformat(new_data["x"][0]))
    t_to = str(datetime.fromisoformat(new_data["x"][-1]))
    new_data["t_from"] = new_data["x"][0]
    new_data["t_to"] = new_data["x"][-1]
    new_data["created_at"] = datetime.now().isoformat()

    # fig = plot_plotly(m, forecast)
    # pio.write_image(fig, CRYPTO_TRANSFER_PREDICTION_PLOT_FILE)

    # with open(CRYPTO_TRANSFER_PREDICTION_PLOT_FILE, rb) as f:
    #     image_data = f.read()
    
    # # Convert image data to base64
    # base64_data = base64.b64encode(image_data).decode("utf-8")
    # md_content = f"![Image](data:image/jpeg;base64,{base64_data})"

    with open(CRYPTO_TRANSFER_PREDICTION_STORAGE_FILE, "w") as f:
        f.write(json.dumps(new_data))

    return MaterializeResult(
        metadata={
            "Prediction start time": new_data["t_from"],
            "Prediction end time": new_data["t_to"],
            "Time prediction was made": new_data["created_at"],
            # "Prediction preview": MetadataValue.md(md_content)
        }
    )


@asset(
    group_name="store_prediction",
    compute_kind="MongoDB",
    deps=["predict_data"]
)
def store_prediction_data():
    """ Store the predictions in MongoDB """

    db_name = "predictions_db"
    collection_name = "predictions"

    mongo_client = pymongo.MongoClient(ATLAS_URI)

    database = mongo_client[db_name]
    collection = database[collection_name]

    with open(CRYPTO_TRANSFER_PREDICTION_STORAGE_FILE, "r") as f:
        predictions = json.loads(f.read())

    returned_id = collection.insert_one(predictions).inserted_id

    return MaterializeResult(
        metadata={
            "Stored prediction id": str(returned_id),
        }
    )