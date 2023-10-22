import os
import boto3
import config_file

class DownloadFiles:
    # AWS credentials and region
    aws_access_key_id = config_file.aws_access_key
    aws_secret_access_key = config_file.aws_secret_access_key
    region_name = 'us-east-1'

    # Create an S3 client using the credentials and region
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

    # S3 bucket and file information
    bucket_name = 'telecomprojectbucket'
    rev_file_name = 'section1/cleaned-csv-files/rev.csv'
    rev_local_file_path = '/opt/airflow/csv_files/rev.csv'

    def fetch_revenue_file_from_aws(self):
        try:
            # Check if the revenue file already exists locally
            if os.path.isfile(self.rev_local_file_path):
                print(f"Rev file already exists locally. Skipping fetching process.")
            else:
                # Create a new local file and download the revenue file from S3
                f = open(self.rev_local_file_path, "x")
                self.s3_client.download_file(self.bucket_name, self.rev_file_name, self.rev_local_file_path)    

            print(f"Successfully fetched files from AWS S3.")

        except Exception as e:
            print(f"Error fetching files from AWS S3: {e}")
