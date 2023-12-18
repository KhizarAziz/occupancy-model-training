import boto3
import pandas as pd
from io import StringIO
from typing import List
import json

class S3DataLoader:
    def __init__(self, bucket_name: str, data_dir: str):
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
        self.data_dir = data_dir

    def list_files(self) -> List[str]:
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=self.data_dir)
            return [item['Key'] for item in response.get('Contents', [])]
        except Exception as e:
            print(f"Error listing files: {e}")
            return []

    def read_all_txt_to_df(self) -> pd.DataFrame:
        try:
            all_files = self.list_files()
            txt_files = [f for f in all_files if f.endswith('.txt')]
            df_list = []

            for file in txt_files:
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file)
                df_list.append(pd.read_csv(StringIO(response['Body'].read().decode('utf-8'))))

            return pd.concat(df_list, ignore_index=True)
        except Exception as e:
            print(f"Error reading txt files to DataFrame: {e}")
            return pd.DataFrame()

    def read_json_to_dict(self, file_key: str) -> dict:
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            json_content = json.loads(response['Body'].read().decode('utf-8'))
            return json_content
        except Exception as e:
            print(f"Error reading JSON file: {e}")
            return {}
