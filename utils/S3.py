import os
import boto3
import pandas as pd
from io import BytesIO

class S3:
    def __init__(self, region=None):
        self.region = region  # Assign the region to the class attribute
        try:
            if region is None:
                self.s3_client = boto3.client('s3')
            else:
                self.s3_client = boto3.client('s3', region_name=region)
                self.location = {'LocationConstraint': region}
        except Exception as e:
            print(e)

    def create_bucket(self, bucket_name):
        """Create an S3 bucket in a specified region"""
        try:
            if self.region is None or self.region == 'us-east-1':
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                self.s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=self.location)
        except Exception as e:
            print(e)

    def list_buckets(self):
        """List all S3 buckets"""
        try:
            response = self.s3_client.list_buckets()
            for bucket in response['Buckets']:
                print(f'Bucket Name: {bucket["Name"]}')
        except Exception as e:
            print(e)
    
    def upload_single_file(self, bucket_name, csv_file_path, s3_key):
        # Latter also add if else for different file formats
        """Upload a file to an S3 bucket"""
        try:
            df = pd.read_csv(csv_file_path)
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            
            response = self.s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=parquet_buffer.getvalue())
            print(f'File uploaded successfully. Response: {response}')
        except Exception as e:
            print(e)
    
    def upload_multiple_files(self, bucket_name, directory_path, s3_prefix):
        """Upload multiple CSV files as Parquet format to the S3 bucket"""
        try:
            file_list = [os.path.join(directory_path, file) for file in os.listdir(directory_path) if file.endswith('.csv')]
            for file_path in file_list:
                file_name = os.path.basename(file_path).replace('.csv', '.parquet')
                s3_key = f'{s3_prefix}/{file_name}'
                self.upload_single_file(bucket_name, file_path, s3_key)
        except Exception as e:
            print(e)
    
    def list_objects(self, bucket_name, prefix):
        """List all objects in an S3 bucket"""
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            if 'Contents' in response:
                for obj in response['Contents']:
                    print(f'Object Name: {obj["Key"]}')
            else:
                print('No objects found')
        except Exception as e:
            print(e)

    def load_objects_to_dataframe(self, bucket_name, prefix):
            """Load objects from an S3 bucket into a pandas DataFrame"""
            try:
                response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
                if 'Contents' in response:
                    data = []
                    for obj in response['Contents']:
                        data.append(obj['Key'])
                    df = pd.DataFrame(data, columns=['Object Key'])
                    return df
                else:
                    print('No objects found')
                    return pd.DataFrame(columns=['Object Key'])
            except Exception as e:
                print(e)
                return pd.DataFrame(columns=['Object Key'])
            

    def read_file_content(self, bucket_name, s3_key):
        """Read the content of a file in an S3 bucket and return it as a pandas DataFrame"""
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            content = response['Body'].read()
            df = pd.read_parquet(BytesIO(content))
            return df
        except Exception as e:
            print(e)
            return pd.DataFrame()

if __name__ == '__main__':
    s3 = S3(region='us-east-1')
    #s3.create_bucket('home-credit-risk')
    #s3.list_buckets()
    file_df = s3.read_file_content('home-credit-risk', 'data/application_train.parquet')
    print(file_df)