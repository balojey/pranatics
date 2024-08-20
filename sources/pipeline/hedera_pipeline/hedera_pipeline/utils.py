from typing import Union
from datetime import datetime
import requests

from .constants import TRANSACTIONS_API_URL


def get_transactions(start_time: float | int, limit: int, transaction_type: str, order: Union["asc", "desc"]):
        url = TRANSACTIONS_API_URL.format(start_time=start_time, limit=limit, transaction_type=transaction_type, order=order)
        data = requests.get(url).json()["transactions"]
        new_transactions: list[dict | None] = []
        for transaction in data:
            new_data = {}
            if not transaction["scheduled"] and transaction["result"] == "SUCCESS":
                _, seconds, nanoseconds = transaction["transaction_id"].split("-")
                new_data["charged_tx_fee"] = round(transaction["charged_tx_fee"] / 1000000, 4)
                new_data["time_tx_occured"] = datetime.fromtimestamp(float(f"{seconds}.{nanoseconds}")).isoformat()

                new_transactions.append(new_data)
        return new_transactions