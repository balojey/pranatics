import pymongo
from dotenv import dotenv_values, find_dotenv
from pprint import pprint

config = dotenv_values(find_dotenv())

try:
    atlas_uri = config.get("ATLAS_URI")
except:
    print("atlas uri not found")

db_name = "predictions_db"
collection_name = "predictions"

mongo_client = pymongo.MongoClient(atlas_uri)

database = mongo_client[db_name]
collection = database[collection_name]


def refine_data(data: dict):
    new_data = {
        "data": [
            {"x": data["x"], "y": data["y"], "type": "scatter"}
        ],
        "title": "Predictions over the next one hour",
        "description": "This is a predictive analytics dashboard for transactions on the hedera network",
        "type": "scatter",
        "t_to": data["t_to"],
        "t_from": data["t_from"],
    }
    
    return new_data


def get_predictions(timestamp_field: str = "created_at", limit: int = 5) -> list[dict]:
    data = list(collection.find().sort([(timestamp_field, -1)]).limit(limit))
    new_data: list[dict] = [refine_data(d) for d in data]

    return new_data


if __name__ == "__main__":
    get_predictions()