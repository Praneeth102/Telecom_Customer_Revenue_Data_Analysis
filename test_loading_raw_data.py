import unittest
from unittest.mock import MagicMock
from unittest.mock import patch
from pyspark.sql import SparkSession
from loading_raw_data import S3FileChecker
from loading_raw_data import SparkSessionCreationSingleton
from loading_raw_data import LoadingDataAWS
from cleaning_device_data import DeviceDataCleaning
from cleaning_crm_data import CRMDataCleaning
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from uploading_into_mongodb_pyspark import UploadingtoMongoDBPyspark
from uploading_into_aws_mongodb_using_pandas import UploadingintoAWSMongodbMulti
import os
from dotenv import load_dotenv
load_dotenv()


# class MockSparkSession(MagicMock):
#     def __init__(self):
#         super().__init__()


"""
=> This SparkSessionCreationSingletonUnitTests used to running unittests.
"""
class SparkSessionCreationSingletonUnitTests(unittest.TestCase):
    """
    => test_spark_session function spark session
    => asserts it is a instance of spark or not
    """
    def test_spark_session(self):
        spark1 = SparkSessionCreationSingleton()
        self.assertIsInstance(spark1, SparkSession)
        


    """
    => This test_spark_sessions_same function creates two session.
    => checks two sessions are refer to same object or not.
    """
    def test_spark_sessions_same(self):
        spark1 = SparkSessionCreationSingleton()
        spark2 = SparkSessionCreationSingleton()
        self.assertIs(spark1, spark2)

class S3FileCheckerUnitTests(unittest.TestCase):
    """
       => Creating S3 mock client
       => Replacing s3 client with mock
    """
    def setUp(self):
        self.mock_s3_client = MagicMock()
        self.s3_file_checker = S3FileChecker()
        self.s3_file_checker.s3_client = self.mock_s3_client
    """
    => mocking head object reponse for file exists
    => checking or asserting result
    """
    def test_file_exists(self):
        self.mock_s3_client.head_object.return_value = {}
        result = self.s3_file_checker.AWS_File_Exist_OR_Not("file_key")
        self.assertTrue(result)

    """
     => This test case is for checking file not found case.
     =>Mocking head object response to file not found case.
     => checking or validating result.
    """
    def test_file_not_exists(self):
        self.mock_s3_client.head_object.side_effect = Exception("File not found")
        result = self.s3_file_checker.AWS_File_Exist_OR_Not("file_key")
        self.assertFalse(result)

"""
=> This class tests the whether data is loading successfully or not.
=> In this we are using patches where ever required.
=> assertEqual,assert_called_once_with are used to verify the data loading
"""
class LoadingDataAWSTest(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSessionCreationSingleton()
        self.loading_data = LoadingDataAWS()


    """
    => This below test case verifies the loading crm from aws.
    => Here we created two patch decorators one is for dataframe and other is for mocking s3_file_checker
    """
    @patch('loading_raw_data.S3FileChecker.AWS_File_Exist_OR_Not')
    @patch('pyspark.sql.DataFrameReader.options')
    def test_loading_crm_data_exists(self, mock_options,mock_s3_file_checker):
        mock_s3_file_checker.return_value = True
        mock_options.return_value.csv.return_value = self.spark.createDataFrame([(1, 'A'), (2, 'B')], ['ID', 'Name'])
        downloader = LoadingDataAWS()
        result = downloader.Loading_crm_data()
        self.assertTrue(mock_s3_file_checker.called)
        mock_options.assert_called_once_with(inferSchema=True, header=True)
        mock_options.return_value.csv.assert_called_once_with("s3a://telecomprojectbucket/RAW_CSV_DATA/crm1.csv")
        self.assertEqual(result.count(), 2)
        self.assertEqual(result.columns, ['ID', 'Name'])
    """
    => This below test case verifies the loading device data from aws.
    => Here we created two patch decorators one is for dataframe and other is for mocking s3_file_checker
    """
    @patch('loading_raw_data.S3FileChecker.AWS_File_Exist_OR_Not')
    @patch('pyspark.sql.DataFrameReader.options')
    def test_loading_device_data_exists(self, mock_options,mock_s3_file_checker):
        mock_s3_file_checker.return_value = True
        mock_options.return_value.csv.return_value = self.spark.createDataFrame([(1, 'A'), (2, 'B')], ['ID', 'Name'])
        downloader = LoadingDataAWS()
        result = downloader.Loading_device_data()
        self.assertTrue(mock_s3_file_checker.called)
        mock_options.assert_called_once_with(inferSchema=True, header=True)
        mock_options.return_value.csv.assert_called_once_with("s3a://telecomprojectbucket/RAW_CSV_DATA/device1.csv")
        self.assertEqual(result.count(), 2)
        self.assertEqual(result.columns, ['ID', 'Name'])

"""
=> This class checks cleaning of device data taking place as our expectations or not
=> This class checks all steps and verify does retrurn dataframe is as expected
"""
class DeviceDataCleaningTest(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSessionCreationSingleton()
        self.device_data_cleaning = DeviceDataCleaning()
    """
    => This function test null values are replaced or not.
    => first we send the testing dataframe to the function then check all null values are replaced or not by using count and filter functions.
    """
    def test_replacing_null_values(self):
        data = [("Samsung", None, "Android", None),
                (None, "Galaxy S10", None, "Samsung"),
                ("Apple", "iPhone 12", "iOS", None)]
        schema = ["brand_name", "model_name", "os_name", "os_vendor"]
        device_df = self.spark.createDataFrame(data, schema=schema)
        cleaned_device_df = self.device_data_cleaning.Replacing_Null_Values(device_df)
        null_columns = ["brand_name", "model_name", "os_name", "os_vendor"]
        for column in null_columns:
            self.assertFalse(cleaned_device_df.filter(col(column).isNull()).count() > 0)


    """
    => this function checks whether "Making_MSISDN_Primary" function coverting given dataframe into msisdn as a primary key.
    => By using distinct and count function we verified it.
    """
    def test_Making_MSISDN_Primary(self):
        data = [("c0e80ecc67484f293db0cf723146c9d6", "7ce90a5469d6a07dc8c770956cc29482", "SAMSUNG", "GALAXY J1 ACE (SM-J111F)","Android","Google"),
                ("c0e80ecc67484f293db0cf723146c9d6", "7ce90a5469d6a07dc8c770956cffred2", None, "GALAXY J1 ACE (SM-J111F)",None,"Google"),
                ("c1230ecc67484f293db0cf723146c9d6", "7ce90a5469d6a07dc8c770956cc29482", "SAMSUNG", "GALAXY J1 ACE (SM-J111F)","Android","Google")]
        schema = ["msisdn","imei_tac","brand_name", "model_name", "os_name", "os_vendor"]
        device_df = self.spark.createDataFrame(data, schema=schema)
        device_cleaned = self.device_data_cleaning.Making_MSISDN_Primary(device_df)
        msisdn_count = device_cleaned.select("msisdn").distinct().count()
        device_count = device_cleaned.count()
        self.assertEqual(msisdn_count, device_count)
    """
    => this function checks whether "Making_MSISDN_Primary" function coverting given dataframe into msisdn as a primary key.
    => and whether it considering expected rows or not.
    => By asserting it with the expected data we verified it.
    """
    def test_Making_MSISDN_Primary1(self):
        data = [("c0e80ecc67484f293db0cf723146c9d6", "7ce90a5469d6a07dc8c770956cc29482", "SAMSUNG", "GALAXY J1 ACE (SM-J111F)","Android","Google"),
                ("c0e80ecc67484f293db0cf723146c9d6", "7ce90a5469d6a07dc8c770956cffred2", None, "GALAXY J1 ACE (SM-J111F)",None,"Google"),
                ("c1230ecc67484f293db0cf723146c9d6", "7ce90a5469d6a07dc8c770956cc29482", "SAMSUNG", "GALAXY J1 ACE (SM-J111F)","Android","Google")]
        schema = ["msisdn","imei_tac","brand_name", "model_name", "os_name", "os_vendor"]
        device_df = self.spark.createDataFrame(data, schema=schema)
        device_cleaned = self.device_data_cleaning.Making_MSISDN_Primary(device_df)
        expected_data = [("c0e80ecc67484f293db0cf723146c9d6", "7ce90a5469d6a07dc8c770956cc29482", "SAMSUNG", "GALAXY J1 ACE (SM-J111F)","Android","Google"),
                ("c1230ecc67484f293db0cf723146c9d6", "7ce90a5469d6a07dc8c770956cc29482", "SAMSUNG", "GALAXY J1 ACE (SM-J111F)","Android","Google")]
        expected_df = self.spark.createDataFrame(expected_data, schema=schema)
        self.assertEquals(device_cleaned.collect(), expected_df.collect())
    """
    => This function checks whether col names and values are replaced in standardize format or not.
    => By asserting it with the expected dataframe it verfies it.
    """
    def test_Replacing_Col_Names_Values(self):
        data = [("c0e80ecc67484f293db0cf723146c9d6", "7ce90a5469d6a07dc8c770956cc29482", "SAMSUNG", "GALAXY J1 ACE (SM-J111F)","Android","Google"),
                ("c1230ecc67484f293db0cf723146c9d6", "7ce90a5469d6a07dc8c770956cc29482", "SAMSUNG", "GALAXY J1 ACE (SM-J111F)","Android","Google")]
        schema = ["msisdn","imei_tac","brand_name", "model_name", "os_name", "os_vendor"]
        device_df = self.spark.createDataFrame(data, schema=schema)
        device_cleaned = self.device_data_cleaning.Replacing_Col_Names_Values(device_df)
        expected_data = [("c0e80ecc67484f293db0cf723146c9d6", "7ce90a5469d6a07dc8c770956cc29482", "SAMSUNG", "GALAXY J1 ACE (SM-J111F)","ANDROID","GOOGLE"),
                ("c1230ecc67484f293db0cf723146c9d6", "7ce90a5469d6a07dc8c770956cc29482", "SAMSUNG", "GALAXY J1 ACE (SM-J111F)","ANDROID","GOOGLE")]
        schema = ["MSISDN","IMEI_TAC","BRAND_NAME", "MODEL_NAME", "OS_NAME", "OS_VENDOR"]
        expected_df = self.spark.createDataFrame(expected_data, schema=schema)
        self.assertEquals(device_cleaned.collect(), expected_df.collect())

class CRMDataCleaningTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSessionCreationSingleton()
        cls.crm_data_cleaning = CRMDataCleaning()

    """
    => this function verfies all null values are replaced or not.
    => It asserts the expected dataframe and return dataframe from the function.
    """
    def test_Casting_and_Replacing_Null_Values(self):
        data = [("aeef4233d9ad", "MALE", 1985.0, "ACTIVE", "Prepaid","Tier_3"),
                ("aeef4233sad", "MALE", 1985.0, "ACTIVE", "Prepaid","Tier_3"),
                ("aeef423229ad", None, 1985.0, "ACTIVE", "Prepaid","Tier_3"),
                ("aeef423dd9ad", "MALE", 1985.0, "ACTIVE", "Prepaid","Tier_3")]

        schema = ["msisdn", "gender", "year_of_birth", "system_status","mobile_type","value_segment"]
        crm_df = self.spark.createDataFrame(data, schema=schema)
        cleaned_crm_df = self.crm_data_cleaning.Casting_and_Replacing_Null_Values(crm_df)
        schema = ["msisdn", "gender", "year_of_birth", "system_status","mobile_type","value_segment"]
        self.assertEqual(cleaned_crm_df.schema["year_of_birth"].dataType, IntegerType())
        expected_data = [("aeef4233d9ad", "MALE", 1985, "ACTIVE", "Prepaid","Tier_3"),
                ("aeef4233sad", "MALE", 1985, "ACTIVE", "Prepaid","Tier_3"),
                ("aeef423229ad", "NOT_AVAILABLE", 1985, "ACTIVE", "Prepaid","Tier_3"),
                ("aeef423dd9ad", "MALE", 1985, "ACTIVE", "Prepaid","Tier_3")]
        expected_crm_df = self.spark.createDataFrame(expected_data, schema=schema)  
        self.assertEquals(expected_crm_df.collect(), cleaned_crm_df.collect())
    """
    => This function checks gender column is standardize or not as expected.
    => It asserts the return dataframe with the expected dataframe.
    """
    def test_Cleaning_Gender_Column(self):
        data = [("aeef4233d9ad", "M", 1985, "ACTIVE", "Prepaid","Tier_3"),
                ("aeef4233sad", "F", 1985, "ACTIVE", "Prepaid","Tier_3"),
                ("aeef423229ad", "MA", 1985, "ACTIVE", "Prepaid","Tier_3"),
                ("aeef423dd9ad", "MALEFE", 1985, "ACTIVE", "Prepaid","Tier_3")]

        schema = ["msisdn", "gender", "year_of_birth", "system_status","mobile_type","value_segment"]
        crm_df = self.spark.createDataFrame(data, schema=schema)
        cleaned_crm_df = self.crm_data_cleaning.Cleaning_Gender_Column(crm_df)
        schema = ["msisdn", "gender", "year_of_birth", "system_status","mobile_type","value_segment"]

        expected_data = [("aeef4233d9ad", "MALE", 1985, "ACTIVE", "Prepaid","Tier_3"),
                ("aeef4233sad", "FEMALE", 1985, "ACTIVE", "Prepaid","Tier_3"),
                ("aeef423229ad", "MALE", 1985, "ACTIVE", "Prepaid","Tier_3"),
                ("aeef423dd9ad", "FEMALE", 1985, "ACTIVE", "Prepaid","Tier_3")]
        expected_crm_df = self.spark.createDataFrame(expected_data, schema=schema)  
        self.assertEquals(expected_crm_df.collect(), cleaned_crm_df.collect())
    """
    => This function checks all column names and values standardize or not as expected.
    => It asserts the return dataframe with the expected dataframe.
    """
    def test_Replacing_Col_Names_Values(self):
        data = [("aeef4233d9ad", "MALE", 1985, "ACTIVE", "Prepaid","Tier_3"),
                ("aeef4233sad", "FEMALE", 1985, "ACTIVE", "Prepaid","Tier_3"),
                ("aeef423229ad", "MALE", 1985, "ACTIVE", "Prepaid","Tier_3"),
                ("aeef423dd9ad", "FEMALE", 1985, "ACTIVE", "Prepaid","Tier_3")]

        schema = ["msisdn", "gender", "year_of_birth", "system_status","mobile_type","value_segment"]
        crm_df = self.spark.createDataFrame(data, schema=schema)
        cleaned_crm_df = self.crm_data_cleaning.Replacing_Col_Names_Values(crm_df)
        schema = ["MSISDN", "GENDER", "YEAR_OF_BIRTH", "SYSTEM_STATUS","MOBILE_TYPE","VALUE_SEGMENT"]

        expected_data = [("aeef4233d9ad", "MALE", 1985, "ACTIVE", "PREPAID","TIER_3"),
                ("aeef4233sad", "FEMALE", 1985, "ACTIVE", "PREPAID","TIER_3"),
                ("aeef423229ad", "MALE", 1985, "ACTIVE", "PREPAID","TIER_3"),
                ("aeef423dd9ad", "FEMALE", 1985, "ACTIVE", "PREPAID","TIER_3")]
        expected_crm_df = self.spark.createDataFrame(expected_data, schema=schema)  
        self.assertEquals(expected_crm_df.collect(), cleaned_crm_df.collect())

"""
=> This class verfies data is being ingested into aws or not.
"""
class UploadingintoAWSMongodbMultiTest(unittest.TestCase):
    """
    => In this class we will create two patches one is for boto session and another is for mock session
    => It verifies data is uploaded into aws or not.
    """
    @patch('uploading_into_aws_mongodb_using_pandas.boto3.Session')
    @patch('loading_raw_data.SparkSessionCreationSingleton')
    def test_upload_to_s3bucket(self, mock_spark, mock_session):
        mock_session_instance = mock_session.return_value
        mock_data = MagicMock()
        mock_data.toPandas.return_value = 'test_data'
        upload = UploadingintoAWSMongodbMulti()
        upload.session = mock_session_instance
        upload.Upload_into_AWS('test_file', mock_data)
        mock_data.toPandas.assert_called_once()
        mock_session.assert_called_once()
        mock_session_instance.client.assert_called_once_with('s3')

if __name__ == '__main__':
    unittest.main()
