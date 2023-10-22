import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,isnan, when, count, expr, length, upper, row_number, countDistinct, size, collect_set, regexp_replace
from pymongo import MongoClient
import os
import json
import subprocess
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
import sys
from dotenv import load_dotenv
from loading_raw_data import LoadingDataAWS
load_dotenv()
"""
=> This class cleans the device raw data.
=> Final Dataframe contains msisdn as primary key.
"""
class DeviceDataCleaning():
 
    """
    => This Replacing_Null_Values function replaces all null values with NOT AVAILABLE.
    => This function consider device dataframe as input.
    => Returns dataframe without null values.
    """
    def Replacing_Null_Values(self,device):
        try:
            if device.count() == 0:
                return device
            print("replacing null values")
            device=device.fillna(value="NOT_AVAILABLE",subset=["brand_name","model_name","os_name","os_vendor"])   
            return device
        except Exception as e:
            print("Failed while replacing null values with zeros in device data: ",str(e))
    """
    => This Making_MSISDN_Primary function makes msisdn as primary key.
    => This function drops dublicated msisdn rows.
    => This function considers rows with less not avaliable elements.
    => By using partitionBy window function it selects the best rows.
    => It return a device dataframe with msisdn as primary key.
    """
    def Making_MSISDN_Primary(self,device):
        try:
            print("device msisdn primary")
            windowSpecifications = Window.partitionBy("msisdn").orderBy(
                    when(col("brand_name") == "NOT_AVAILABLE", 1).otherwise(0),
                    when(col("model_name") == "NOT_AVAILABLE", 1).otherwise(0),
                    when(col("os_name") == "NOT_AVAILABLE", 1).otherwise(0),
                    when(col("os_vendor") == "NOT_AVAILABLE", 1).otherwise(0)
                )
            device = device.withColumn("row_number", row_number().over(windowSpecifications))
            device = device.filter(col("row_number") == 1)
            device= device.drop("row_number")
            return device
        except Exception as e:
            print("Failed while making MSISDN as primary in device data: ",str(e))
    
    """
    => This function replaces column names into systematic format.
    => All column values are replaces in upper letter.
    => It takes dataframe as input,
    => Returns updated dataframe as output.
    """
    def Replacing_Col_Names_Values(self,device):
        try:
            print("replaces column names into systematic format")
            device = device.withColumnRenamed("msisdn","MSISDN")
            device = device.withColumnRenamed("imei_tac","IMEI_TAC")
            device = device.withColumnRenamed("brand_name","BRAND_NAME")
            device = device.withColumnRenamed("model_name","MODEL_NAME")
            device = device.withColumnRenamed("os_name","OS_NAME")
            device = device.withColumnRenamed("os_vendor","OS_VENDOR")

            device = device.withColumn("BRAND_NAME",upper(col("BRAND_NAME")))
            device = device.withColumn("MODEL_NAME",upper(col("MODEL_NAME")))
            device = device.withColumn("OS_NAME",upper(col("OS_NAME")))
            device = device.withColumn("OS_VENDOR",upper(col("OS_VENDOR")))
            return device
        except Exception as e:
            print("Failed to  replace device col values and names:",str(e))

    """
    => This function converts cleaned dataframe into json list.
    => First it calls above cleaning functions.
    => Then it converts into json list.
    => This function returns json list of cleaned device data.
    """
    def Converting_into_json_Data(self):
        device_from_aws = LoadingDataAWS()
        device = device_from_aws.Loading_device_data()
        device = self.Replacing_Null_Values(device)
        device = self.Making_MSISDN_Primary(device)
        device = self.Replacing_Col_Names_Values(device)
        try:
            data_list_json = device.toJSON().map(lambda str_json: json.loads(str_json)).collect() 
            return data_list_json
        except Exception as e:
            print("Failed while converting device data into Json:", str(e))

    """
    => First it loads the raw dataframe then it calls cleaning functions.
    => This Return_Device_DF function returns cleaned device dataframe.
    """
    def Return_Device_DF(self):
        device_from_aws = LoadingDataAWS()
        device = device_from_aws.Loading_device_data()
        device = self.Replacing_Null_Values(device)
        device = self.Making_MSISDN_Primary(device)
        device = self.Replacing_Col_Names_Values(device)
        return device
    
