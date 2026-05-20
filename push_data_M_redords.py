from pymongo import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import BulkWriteError
from urllib.parse import quote_plus
from dotenv import load_dotenv
from pathlib import Path
import os
import sys
import certifi
import pandas as pd
import json
import time

from networkSecurity.exception.exception import NetworkSecurityException
from networkSecurity.logging.logger import logging


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env", override=True)

username = os.getenv("MONGO_USERNAME")
password = os.getenv("MONGO_PASSWORD")

if not username or not password:
    raise ValueError("MongoDB username or password is missing in .env file")

username = quote_plus(username)
password = quote_plus(password)

MONGO_DB_URL = f"mongodb+srv://{username}:{password}@cluster0.1jzj3ek.mongodb.net/?appName=Cluster0"

ca = certifi.where()


class NetworkDataETLPipeline:

    def __init__(self):
        try:
            self.mongo_client = MongoClient(
                MONGO_DB_URL,
                tlsCAFile=ca,
                server_api=ServerApi("1")
            )

            self.mongo_client.admin.command("ping")
            logging.info("MongoDB connection successful")
            print("MongoDB connection successful")

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def get_checkpoint_file(self, file_path):
        file_name = Path(file_path).stem
        return BASE_DIR / f"{file_name}_checkpoint.json"

    def load_checkpoint(self, checkpoint_file):
        if checkpoint_file.exists():
            with open(checkpoint_file, "r") as file:
                data = json.load(file)
                return data.get("last_completed_chunk", 0)

        return 0

    def save_checkpoint(self, checkpoint_file, chunk_number, total_inserted):
        data = {
            "last_completed_chunk": chunk_number,
            "total_inserted": total_inserted,
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        with open(checkpoint_file, "w") as file:
            json.dump(data, file, indent=4)

    def transform_chunk(self, chunk, file_name, start_row_number):
        try:
            chunk.reset_index(drop=True, inplace=True)

            # Replace NaN values with None because MongoDB stores None as null
            chunk = chunk.where(pd.notnull(chunk), None)

            records = chunk.to_dict(orient="records")

            transformed_records = []

            for index, record in enumerate(records):
                row_number = start_row_number + index

                # Stable _id helps resume safely.
                # If the script stops and you rerun it, MongoDB can skip duplicates.
                record["_id"] = f"{file_name}_{row_number}"
                record["source_file"] = file_name
                record["source_row_number"] = row_number

                transformed_records.append(record)

            return transformed_records

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def load_records(self, records, database_name, collection_name):
        try:
            if not records:
                return 0

            db = self.mongo_client[database_name]
            collection = db[collection_name]

            try:
                result = collection.insert_many(records, ordered=False)
                return len(result.inserted_ids)

            except BulkWriteError as e:
                # This handles duplicate _id records during resume.
                inserted_count = e.details.get("nInserted", 0)

                duplicate_errors = [
                    error for error in e.details.get("writeErrors", [])
                    if error.get("code") == 11000
                ]

                other_errors = [
                    error for error in e.details.get("writeErrors", [])
                    if error.get("code") != 11000
                ]

                if other_errors:
                    logging.error(f"Non-duplicate bulk write errors: {other_errors[:5]}")
                    raise

                return inserted_count

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def run_pipeline(
        self,
        file_path,
        database_name,
        collection_name,
        chunksize=10000,
        resume=True
    ):
        try:
            file_path = Path(file_path)
            file_name = file_path.stem

            if not file_path.exists():
                raise FileNotFoundError(f"CSV file not found: {file_path}")

            checkpoint_file = self.get_checkpoint_file(file_path)

            last_completed_chunk = 0

            if resume:
                last_completed_chunk = self.load_checkpoint(checkpoint_file)

            total_inserted = 0

            print(f"Starting ETL pipeline")
            print(f"File: {file_path}")
            print(f"Database: {database_name}")
            print(f"Collection: {collection_name}")
            print(f"Chunk size: {chunksize}")
            print(f"Resume from chunk: {last_completed_chunk + 1}")

            for chunk_number, chunk in enumerate(
                pd.read_csv(file_path, chunksize=chunksize),
                start=1
            ):
                if resume and chunk_number <= last_completed_chunk:
                    continue

                start_row_number = ((chunk_number - 1) * chunksize) + 1

                records = self.transform_chunk(
                    chunk=chunk,
                    file_name=file_name,
                    start_row_number=start_row_number
                )

                inserted_count = self.load_records(
                    records=records,
                    database_name=database_name,
                    collection_name=collection_name
                )

                total_inserted += inserted_count

                self.save_checkpoint(
                    checkpoint_file=checkpoint_file,
                    chunk_number=chunk_number,
                    total_inserted=total_inserted
                )

                print(
                    f"Chunk {chunk_number} completed | "
                    f"Inserted: {inserted_count} | "
                    f"Total inserted this run: {total_inserted}"
                )

            print("ETL pipeline completed successfully")
            print(f"Total inserted this run: {total_inserted}")

            return total_inserted

        except Exception as e:
            raise NetworkSecurityException(e, sys)


if __name__ == "__main__":

    FILE_PATH = BASE_DIR / "Network_Data" / "phisingData.csv"

    DATABASE = "MUSAB"
    COLLECTION = "NetworkData"

    pipeline = NetworkDataETLPipeline()

    total_records = pipeline.run_pipeline(
        file_path=FILE_PATH,
        database_name=DATABASE,
        collection_name=COLLECTION,
        chunksize=10000,
        resume=True
    )

    print(f"Final inserted records: {total_records}")