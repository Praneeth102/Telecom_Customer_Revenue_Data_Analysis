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
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from pyspark.sql.functions import udf
load_dotenv()
"""
=>CRMDataCleaning class is for cleaning raw CRM data.
=> This class clean the crm data that we loaded for aws s3 bucket.
"""
class CRMDataCleaning():
    """
    => This Casting_and_Replacing_Null_Values function is used for casting columns and replacing null values with "NOT AVAILABLE"
    => This takes the dataframe as input.
    => This function returns crm dataframe without any null values
    """
    def Casting_and_Replacing_Null_Values(self,crm):
        try:
            print("crm cleaning started")
            print("casting columns and replacing null values with NOT AVAILABLE")
            crm = crm.withColumn("year_of_birth", crm["year_of_birth"].cast(IntegerType()))
            #replacing null values of gender into NOT AVAILABLE
            crm= crm.fillna(value="NOT_AVAILABLE",subset=["gender"])
            #replacing null values of year_of_birth into 0
            crm= crm.fillna(value=0,subset=["year_of_birth"])  
            return crm 
        except Exception as e:
            print("Failed while casting and replacing null values of crm data: ",str(e))

    def applying_fuzzywuzzy(self,crm):
        def clean_gender(value):
            value_upper = value.upper()
            if value_upper in ['MALE', 'FEMALE', 'NOT AVAILABLE', 'OTHERS']:
                return value_upper

            choices = ['MALE', 'FEMALE', 'NOT AVAILABLE', 'OTHERS']
            best_match = process.extractOne(value_upper, choices, scorer=fuzz.token_sort_ratio)
            if best_match[1] >= 75:
                return best_match[0]
            else:
                return 'NOT AVAILABLE'
        fuzzy_match_udf = udf(clean_gender)
        crm = crm.withColumn('gender', fuzzy_match_udf('gender'))
        return crm

    """
    =>This Cleaning_Gender_Column function is used to clean the gender column present in crm dataframe.
    => This takes the dataframe as input.
    => It replaces all the invalid elements with exact elements according to our requirement.
    => By using regex pattern matcher it replaces invalied elements
    => This function return a crm dataframe with cleaned gender column
    """
    def Cleaning_Gender_Column(self,crm):
        try:
            print("cleaning the gender column present in crm dataframe ")
            crm = crm.withColumn("gender", when(((crm.gender == "M") | (upper(crm.gender) == "MALE.") | (upper(crm.gender) == "MALE'")), "MALE").when((crm.gender == "F") | (crm.gender == "FE"), "FEMALE").otherwise(crm.gender)) 
            crm = crm.withColumn("gender", when(upper(col("gender")).like("%FE%") & (length(col("gender")) > 4),"FEMALE").otherwise(crm.gender)) 
            crm = crm.withColumn("gender", when(upper(col("gender")).like("%MA%") & (length(col("gender")) <= 4),"MALE").when(upper(col("gender")).like("%MA%") & (length(col("gender")) > 4),"FEMALE").otherwise(crm.gender)) 
            crm = crm.withColumn("gender", when( (upper(crm.gender) == "OTHER") , "OTHERS").when((crm.gender != "MALE") & (crm.gender != "FEMALE") & (crm.gender != "OTHERS"), "NOT_AVAILABLE").otherwise(crm.gender))
            return crm
        except Exception as e:
            print("Failed while cleaning gender column of crm data: ",str(e))

    """
    => This Making_MSISDN_Primary function is used to drop all the dublicated msisdn rows.
    => This takes the dataframe as input.
    => droping of rows takes place by following certain set of rules.
    => It considers most suitable row from a group of same msisdn.
    => This function returns the crm dataframe with msisdn as primary key.
    """
    def Making_MSISDN_Primary(self,crm):
        try:
            print("msisdn as primary key in crm")
            crm = crm.withColumn("active_count" ,count(when(col("system_status") == "ACTIVE", 1)).over(Window.partitionBy("msisdn","year_of_birth", "mobile_type", "gender"))) \
            .withColumn("suspend_count" ,count(when(col("system_status") == "SUSPEND", 1)).over(Window.partitionBy("msisdn","year_of_birth", "mobile_type", "gender"))) \
            .withColumn("idle_count", count(when(col("system_status") == "IDLE", 1)).over(Window.partitionBy("msisdn","year_of_birth", "mobile_type", "gender")))\
            .withColumn("deactive_count", count(when(col("system_status") == "DEACTIVE", 1)).over(Window.partitionBy("msisdn","year_of_birth", "mobile_type", "gender")))

            windowSpec = Window.partitionBy("msisdn").orderBy(
                when((col("deactive_count") == 1) & (col("system_status") == 'DEACTIVE'), 0).otherwise(1),
                when((col("active_count") > col("suspend_count")) & (col("suspend_count") != 0) & (col("system_status") == 'ACTIVE'), 0) \
                .when((col("active_count") > col("idle_count")) & (col("idle_count") != 0) & (col("system_status") == 'ACTIVE'), 0) \
                .when((col("active_count") > 0) & (col("suspend_count") == 0) & (col("idle_count") == 0) & (col("system_status") == 'ACTIVE'), 0) \
                .when((col("active_count") < col("suspend_count")) & (col("system_status") == 'SUSPEND'), 0) \
                .when((col("active_count") == col("suspend_count")) & (col("system_status") == 'SUSPEND'), 1) \
                .when((col("active_count") == col("idle_count")) & (col("system_status") == 'IDLE'), 1) \
                .when((col("active_count") == col("idle_count") + col("suspend_count")) & (col("suspend_count") != 0) & (col("idle_count") != 0) & (col("system_status") == 'SUSPEND') , 1) \
                .otherwise(2),
                when(col("year_of_birth") == 0, 1).otherwise(0), 
                when((col("gender") != 'MALE') & (col("gender") != 'FEMALE') & (col("gender") != 'OTHERS'), 1).otherwise(0)
            )
            crm = crm.withColumn("row_number", row_number().over(windowSpec))
            crm = crm.filter(col("row_number") == 1)
            crm= crm.drop("active_count","suspend_count","idle_count","deactive_count","row_number")
            return crm
        except Exception as e:
            print("Failed while making msisdn primary key of crm data:",str(e))

    def Replacing_Col_Names_Values(self,crm):
        try: 
            print("replacing col names and values in crm")
            crm = crm.withColumnRenamed("msisdn","MSISDN")
            crm = crm.withColumnRenamed("gender","GENDER")
            crm = crm.withColumnRenamed("year_of_birth","YEAR_OF_BIRTH")
            crm = crm.withColumnRenamed("system_status","SYSTEM_STATUS")
            crm = crm.withColumnRenamed("mobile_type","MOBILE_TYPE")
            crm = crm.withColumnRenamed("value_segment","VALUE_SEGMENT")

            crm = crm.withColumn("GENDER",upper(col("GENDER")))
            crm = crm.withColumn("SYSTEM_STATUS",upper(col("SYSTEM_STATUS")))
            crm = crm.withColumn("MOBILE_TYPE",upper(col("MOBILE_TYPE")))
            crm = crm.withColumn("VALUE_SEGMENT",upper(col("VALUE_SEGMENT")))
            return crm
        except Exception as e:
            print("Failed to  replace crm col values and names:",str(e))
    """
    => This Converting_into_json function loads the crm raw data. Then it calls above cleaning function.
    => After cleaning is done it converts the dataframe into json list.
    => This function returns the json list of crm data.
    """
    def Converting_into_json(self):
        crm_from_aws = LoadingDataAWS()
        crm = crm_from_aws.Loading_crm_data()
        crm = self.Casting_and_Replacing_Null_Values(crm)
        crm = self.Cleaning_Gender_Column(crm)
        crm = self.Making_MSISDN_Primary(crm)
        crm = self.Replacing_Col_Names_Values(crm)
        try:
            data_list_json = crm.toJSON().map(lambda str_json: json.loads(str_json)).collect()     
            return data_list_json
        except Exception as e:
            print("Failed while converting crm data into json: ",str(e))

    """
    => This Return_CRM_DF function loads the crm raw data. Then it calls above cleaning function.
    => This function returns the cleaned dataframe of crm data.
    """
    def Return_CRM_DF(self):
        crm_from_aws = LoadingDataAWS()
        crm = crm_from_aws.Loading_crm_data()
        crm = self.Casting_and_Replacing_Null_Values(crm)
        crm = self.Cleaning_Gender_Column(crm)
        crm = self.Making_MSISDN_Primary(crm)
        crm = self.Replacing_Col_Names_Values(crm)
        return crm