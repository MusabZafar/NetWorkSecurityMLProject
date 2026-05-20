from pymongo import MongoClient
from pymongo.server_api import ServerApi
from urllib.parse import quote_plus
from dotenv import load_dotenv
from pathlib import Path
import os
import sys
import certifi
import pandas as pd

from networkSecurity.exception.exception import NetworkSecurityException
from networkSecurity.logging.logger import logging


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

username = os.getenv("MONGO_USERNAME")
password = os.getenv("MONGO_PASSWORD")

if not username or not password:
    raise ValueError("MongoDB username or password is missing in .env file")

username = quote_plus(username)
password = quote_plus(password)

MONGO_DB_URL = f"mongodb+srv://{username}:{password}@cluster0.1jzj3ek.mongodb.net/?appName=Cluster0"

ca = certifi.where()


class NetworkDataExtract:

    def __init__(self):
        try:
            self.mongo_client = MongoClient(
                MONGO_DB_URL,
                tlsCAFile=ca,
                server_api=ServerApi("1")
            )

            self.mongo_client.admin.command("ping")
            logging.info("MongoDB connection successful")

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def csv_to_json_convertor(self, file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)

            records = data.to_dict(orient="records")
            return records

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def insert_data_mongodb(self, records, database, collection):
        try:
            if len(records) == 0:
                return 0

            db = self.mongo_client[database]
            mongo_collection = db[collection]

            result = mongo_collection.insert_many(records)

            return len(result.inserted_ids)

        except Exception as e:
            raise NetworkSecurityException(e, sys)


if __name__ == "__main__":
    FILE_PATH = BASE_DIR / "Network_Data" / "phisingData.csv"

    DATABASE = "MUSAB"
    COLLECTION = "NetworkData"

    networkobj = NetworkDataExtract()

    records = networkobj.csv_to_json_convertor(file_path=FILE_PATH)

    print(records[:5])

    no_of_records = networkobj.insert_data_mongodb(
        records,
        DATABASE,
        COLLECTION
    )

    print(no_of_records)