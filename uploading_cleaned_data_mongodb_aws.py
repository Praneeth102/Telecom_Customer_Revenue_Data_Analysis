import os
from dotenv import load_dotenv
from loading_raw_data import SparkSessionCreationSingleton
from pyspark.sql.functions import col
load_dotenv()

"""
=> This class uploads the cleaned data into aws loading it from the mongodb.
=> By using pyspark it loads and ingest the data.
"""
class UploadingDataMongoDBAWS():
    def __init__(self):
        self.mongo_uri = os.getenv("MONGODB_URL")
        self.database = os.getenv("DATABASE_NAME")
        self.spark = SparkSessionCreationSingleton()
    """
    This function loads the crm data from mongodb database using pyspark
    One data is loaded we will drop the unwanted col.
    """
    def Reading_CRMDF_Mongodb(self):
        collection = os.getenv("CRM_COLLECTION")
        crm_df = self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database",self.database).option("collection", collection).load()
        crm_df = crm_df.drop(col("_id"))
        return crm_df
    """
    This function uploads the device data into aws s3 storage using pyspark
    """
    def Reading_DeviceDF_Mongodb(self):
        collection = os.getenv("DEVICE_COLLECTION")
        device_df = self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database",self.database).option("collection", collection).load()
        device_df = device_df.drop(col("_id"))
        return device_df

    """
    This function loads the device data from mongodb database using pyspark
    One data is loaded we will drop the unwanted col.
    """
    def Uploading_crm_into_aws(self):
        crm = self.Reading_CRMDF_Mongodb()
        crm_path = os.getenv("CLEANED_CRM_AWS_PATH")
        crm.write.format('csv').option('header','true').option("fs.s3a.committer.name", "partitioned").save(crm_path,mode='overwrite')
    """
    This function uploads the device data into aws s3 storage using pyspark
    """
    def Uploading_device_into_aws(self):
        device = self.Reading_DeviceDF_Mongodb()
        device_path = os.getenv("CLEANED_DEVICE_AWS_PATH")
        repartitioned_device = device.coalesce(1)
        repartitioned_device.write.format('csv').option('header','true').option("fs.s3a.committer.name", "partitioned").save(device_path,mode='overwrite')
    
if __name__ == "__main__":
   aws_uploading = UploadingDataMongoDBAWS()
   #aws_uploading.Uploading_crm_into_aws()
   aws_uploading.Uploading_device_into_aws()

    
