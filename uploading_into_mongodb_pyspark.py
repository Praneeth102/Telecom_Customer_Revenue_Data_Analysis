from cleaning_crm_data import CRMDataCleaning
from cleaning_device_data import DeviceDataCleaning
from dotenv import load_dotenv
import os
load_dotenv

"""
=> This UploadingtoMongoDBPyspark class uploads cleaned dataframe into mongodb using spark session. 
"""
class UploadingtoMongoDBPyspark():
    def __init__(self):
        self.database = os.getenv("DATABASE_NAME")
    """
    => This Uploading_CRM_data function uploads crm dataframe into mongodb database.
    => By using spark session data is uploaded into mongodb.
    => It takes cleaned crm dataframe as input.
    """
    def Uploading_CRM_data(self,crm):
        
        collection = os.getenv("CRM_COLLECTION")
        try:
            crm.write.format("com.mongodb.spark.sql.DefaultSource").option("database",self.database).option("collection", collection).save()
            print("crm data loaded into mongodb")
        except Exception as e:
            print("Failed to upload crm data into mongodb using pyspark: ",str(e))
    """
    => This Uploading_device_data function uploads device dataframe into mongodb database.
    => By using spark session data is uploaded into mongodb.
    => It takes cleaned device dataframe as input.
    """
    def Uploading_device_data(self,device):
        collection = os.getenv("DEVICE_COLLECTION")
        try:
            device.write.format("com.mongodb.spark.sql.DefaultSource").option("database",self.database).option("collection", collection).save()
            print("device data uploaded into mongodb")
        except Exception as e:
            print("Failed to upload device data into mongodb using pyspark:",str(e))
    """
    => This calling_functions function calls above functions and make the task to execute.
    => First it loads cleaned dataframe then it pushes into mongodb.
    """
    def calling_functions(self):
        crm_cleaning= CRMDataCleaning()
        crm = crm_cleaning.Return_CRM_DF()
        device_cleaning=DeviceDataCleaning()
        device = device_cleaning.Return_Device_DF()
        self.Uploading_CRM_data(crm)
        self.Uploading_device_data(device)

if __name__ == "__main__":
    upload = UploadingtoMongoDBPyspark()
    upload.calling_functions()