# Importing required Libraries
import pyspark
from pyspark.sql import SparkSession
import os
import json
from dotenv import load_dotenv
import boto3
load_dotenv()

aws_access_key = os.getenv('AWS_ACCESS_KEY')
aws_secret_key = os.getenv('AWS_SECREAT_KEY')
aws_region_name = os.getenv('AWS_REGION')
aws_bucket_name = os.getenv('AWS_BUCKET_NAME')
mongodb_uri = os.getenv("MONGODB_URL")

"""
=> SparkSessionCreationSingleton class is for creating spark session for single time. 
=> Returns same session when ever called again.
"""
class SparkSessionCreationSingleton():
    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance.spark = SparkSession.builder.master("local[*]") \
                .config("spark.driver.memory", "8g") \
                .config("spark.driver.maxResultSize", "4g") \
                .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.12.180") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
                .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
                .config("spark.mongodb.input.uri", mongodb_uri) \
                .config("spark.mongodb.output.uri", mongodb_uri) \
                .appName("Session_for_cleaning_data").getOrCreate()
        return cls._instance.spark
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
            print("File Not found",str(e))
            return False
"""
=> LoadingDataAWS class is used to load the raw data from aws s3 bucket.
=> By using Pyspark it loads the data
"""
class LoadingDataAWS():
    def __init__(self):
        self.spark = SparkSessionCreationSingleton()
        self.raw_crm_aws_path=os.getenv('RAW_CRM_AWS_PATH')
        self.raw_device_aws_path=os.getenv('RAW_DEVICE_AWS_PATH')
        self.s3_file_checker = S3FileChecker()

    """
    => This Loading_crm_data function loads the raw crm data from aws s3 bucket from a given path using spark.
    => This function returns the crm dataframe.
    """
    def Loading_crm_data(self):
        try:
            file_path = "RAW_CSV_DATA/crm1.csv"
            aws_response = self.s3_file_checker.AWS_File_Exist_OR_Not(file_path)

            if aws_response :
                crm_data = self.spark.read.options(inferSchema=True, header=True).csv(self.raw_crm_aws_path)
                print("crm file is loaded from aws")
                return crm_data
            else:
                print("CRM File Not Found")
        except Exception as e:
            print("Failed to load crm data: ",str(e))

    """
    => This Loading_crm_data function loads the device data from aws s3 bucket from a given path using spark.
    => This function returns the device dataframe.
    """
    def Loading_device_data(self):
        try:
            file_path = "RAW_CSV_DATA/device1.csv"
            aws_response = self.s3_file_checker.AWS_File_Exist_OR_Not(file_path)
            if aws_response:
                device_data = self.spark.read.options(inferSchema=True, header=True).csv(self.raw_device_aws_path)
                print("device file is loaded from aws")
                return device_data
            else :
                print("Device File not found")

        except Exception as e:
            print("Failed to load crm data: ",str(e))




