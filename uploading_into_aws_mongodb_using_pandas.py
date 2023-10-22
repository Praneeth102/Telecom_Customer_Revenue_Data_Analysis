import os
from dotenv import load_dotenv
from loading_raw_data import SparkSessionCreationSingleton
import boto3
from io import StringIO
import pandas as pd
from pymongo import MongoClient
import csv
from pyspark.sql.functions import col

load_dotenv()

aws_access_key = os.getenv('AWS_ACCESS_KEY')
aws_secret_key = os.getenv('AWS_SECREAT_KEY')
aws_region_name = os.getenv('AWS_REGION')
aws_bucket_name = os.getenv('AWS_BUCKET_NAME')

"""
=> S3FileChecker Class is used for checking whether given file is present in s3 bucket or not.
=> By using boto it creates a connection with s3 bucket.
=> Returns bool.
"""
class S3FileChecker():
    def __init__(self):
        self.bucket_name = aws_bucket_name
        self.session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region_name
        )
        self.s3_client = self.session.client('s3')
    #This function checks the presence of file
    def AWS_File_Exist_OR_Not(self,file_key):
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=file_key)
            return True
        except Exception as e:
           # print("File Not found",str(e))
            return False
"""
=> This Uploading_into_AWS_Mongodb_Multi class uploads cleaned data from MongoDB to AWS
=> By using spark it reads the data from MongoDB
=> By using Multipart upload it uploads into AWS S3 Bucket.
"""
class UploadingintoAWSMongodbMulti():
    def __init__(self):
        self.mongo_uri = os.getenv("MONGODB_URL")
        self.database = os.getenv("DATABASE_NAME")
        self.spark = SparkSessionCreationSingleton()
        self.bucket_name = aws_bucket_name
        self.session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region_name
        )
       
    """
    => This function reads cleaned crm collection from mongoDB using pyspark.
    => Collection is loaded in dataframe format.
    => After data get loaded it drops _id column
    => It return crm dataframe
    """
    def Reading_CRMDF_Mongodb(self):
        try:
            collection = os.getenv("CRM_COLLECTION")
            crm_df = self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database",self.database).option("collection", collection).load()
            crm_df = crm_df.drop(col("_id"))
            print("crm data loaded from mongodb")
            return crm_df
        except Exception as e:
            print("Failed to read crm data from mongodb:",str(e))

    """
    => This function reads cleaned device collection from mongoDB using pyspark.
    => Collection is loaded in dataframe format.
    => After data get loaded it drops _id column
    => It return device dataframe
    """
    def Reading_DeviceDF_Mongodb(self):
        try:
            collection = os.getenv("DEVICE_COLLECTION")
            device_df = self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database",self.database).option("collection", collection).load()
            device_df = device_df.drop(col("_id"))
            print("device data loaded from mongodb")
            return device_df
        except Exception as e:
            print("Failed to read device data from mongodb:",str(e))

    """
    =>This function uploads dataframe into AWS s3 bucket using multipart upload.
    => File name and dataframe are the input for this function.
    """
    def Upload_into_AWS(self,file,data):
        try:
            destination_path = f'section1/cleaned-csv-files/{file}.csv'
            csv_buffer = StringIO()
            data = data.toPandas()
            data.to_csv(csv_buffer, index=False)
            csv_string = csv_buffer.getvalue()
            print("converted into csv string")
        except Exception as e:
            print(f"Error while converting {file} data into csv string",str(e))

        s3 = self.session.client('s3')
        #creating multipart upload
        response = s3.create_multipart_upload(Bucket=self.bucket_name, Key=destination_path)
        upload_id = response['UploadId']
        
        part_size = 10 * 1024 * 1024  # 10MB

        try:
            #dividing data into small parts
            parts = [csv_string[i:i+part_size] for i in range(0, len(csv_string), part_size)]
            total_parts = len(parts)     
            etags = []   
            for part_number, part_data in enumerate(parts, 1):
                #uploading into s3 bucket part by part
                response = s3.upload_part(
                    Bucket=self.bucket_name,
                    Key=destination_path,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=part_data.encode()
                )
                etags.append(response['ETag'])  
            #completing multipart upload
            response = s3.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=destination_path,
                UploadId=upload_id,
                MultipartUpload={'Parts': [{'ETag': etag, 'PartNumber': part_number} for part_number, etag in enumerate(etags, 1)]}
            )

            print('CSV file uploaded successfully.')

        except Exception as e:
            s3.abort_multipart_upload(Bucket=self.bucket_name, Key=destination_path, UploadId=upload_id)
            print('Error occurred during CSV file upload:', str(e))

    """
    => This function takes file name and dataframe as input.
    => Before uploading into AWS s3 bucket it checks does cleaned data is already uploaded or not.
    => If data is already present according to our input it skips or replaces the file.
    => If no file found simply it uploads the data.
    """
    def Uploading_data_AWS(self,file,data):
        destination_path = f'section1/cleaned-csv-files/{file}.csv'
        filechecker = S3FileChecker()
        response = filechecker.AWS_File_Exist_OR_Not(destination_path)
        if response:
            print(f"Cleaned {file} files already present enter yes to replace files(Y/N) :")
            choice = input()
            if choice == "Y":
               self.Upload_into_AWS(file, data)
            else:
                print(f"Replacing {file} files is stopped")
        else:
            self.Upload_into_AWS(file, data)

    """
    => This function simply calls above functions step wise.
    """
    def calling_mongodb_aws_upload_func(self):
        crm = self.Reading_CRMDF_Mongodb()
        device = self.Reading_DeviceDF_Mongodb()
        self.Uploading_data_AWS("crm", crm)
        self.Uploading_data_AWS("device", device)
if __name__ == "__main__":
    uploader = UploadingintoAWSMongodbMulti()
    uploader.calling_mongodb_aws_upload_func()
