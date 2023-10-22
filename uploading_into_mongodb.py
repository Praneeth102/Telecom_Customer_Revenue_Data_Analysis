from cleaning_crm_data import CRMDataCleaning
from cleaning_device_data import DeviceDataCleaning
from pymongo import MongoClient
import subprocess
import json
import os
import sys
from dotenv import load_dotenv
load_dotenv()

"""This class uploads the cleaned data into mongodb using subprocess"""

class UploadingintoMongoDB():
    """
    => By using mongoclient it establish a connection with mongodb.
    """
    def __init__(self):
        try:
            self.mongo_uri = os.getenv("MONGODB_URL")
            self.database = os.getenv("DATABASE_NAME")
            client = MongoClient(self.mongo_uri)
            db = client[self.database]
        except Exception as e:
            print("Failed while creating connection with mongodb: ",str(e))
    """
    By using subprocess it uploads the data into mongodb
    """
    def Uploading_Data(self,data_list_json,collection):
        try:
            batch_size_bytes = sys.getsizeof(data_list_json)
            command_to_load_data = ["mongoimport", "--db", self.database, "--collection", collection,"--numInsertionWorkers","4","--batchSize", str(batch_size_bytes), "--type", "json", "--jsonArray"]
            subprocess.run(command_to_load_data,input=json.dumps(data_list_json).encode("utf-8"))
        except Exception as e:
            print(f"Failed while uploading  {collection} data into mongodb",str(e))

if __name__ == "__main__":
    crm_data = CRMDataCleaning()
    device_data = DeviceDataCleaning()
    data_uploading = UploadingintoMongoDB()
    data_uploading.Uploading_Data(crm_data.Converting_into_json(),"crm")
    data_uploading.Uploading_Data(device_data.Converting_into_json_Data(), "device")
