
# creating session ==> Loading raw data from aws ==> [Cleaning crm data , cleaning device data] ==> loading into mongo using pyspark  ==> Loading cleaned data from mongodb to aws
# importing required libraries
from uploading_into_mongodb_pyspark import UploadingtoMongoDBPyspark
from uploading_into_aws_mongodb_using_pandas import UploadingintoAWSMongodbMulti

"""
This class executes all steps in an order format.
First it class cleaning tasks then it calls uploading tasks
"""
class CleaningandUploadingMongoDBAWS():
    def step_wise_executor(self):
        upload = UploadingtoMongoDBPyspark()
        upload.calling_functions()
        aws_uploader = UploadingintoAWSMongodbMulti()
        aws_uploader.calling_mongodb_aws_upload_func()

if __name__ == "__main__":
    object_ = CleaningandUploadingMongoDBAWS()
    object_.step_wise_executor()