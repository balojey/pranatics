import pymongo
from constants import ATLAS_URI

db_name = "predictions_db"
collection_name = "predictions"

mongo_client = pymongo.MongoClient(ATLAS_URI)

database = mongo_client[db_name]
collection = database[collection_name]

doc_count = collection.count_documents (filter = {})
print (f"Document count before delete : {doc_count:,}")

result = collection.delete_many(filter= {})
print (f"Deleted docs : {result.deleted_count}")