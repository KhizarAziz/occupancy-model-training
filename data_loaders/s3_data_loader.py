import boto3
import pandas as pd
from io import StringIO
from typing import List
import json

class S3DataLoader:
    def __init__(self, bucket_name: str,data_dir: str):
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
        self.data_dir = data_dir

    def list_files(self) -> List[str]:
        """
        List all files in the specified S3 bucket.
        
        Returns:
            List[str]: A list of file names.
        """
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=self.data_dir)
        return [item['Key'] for item in response.get('Contents', [])]

    def read_all_txt_to_df(self) -> pd.DataFrame:
        """
        Reads all .txt files in the bucket into a single DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing the combined data from all .txt files.
        """
        all_files = self.list_files()
        txt_files = [f for f in all_files if f.endswith('.txt')]
        df_list = []

        for file in txt_files:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file)
            df_list.append(pd.read_csv(StringIO(response['Body'].read().decode('utf-8'))))

        return pd.concat(df_list, ignore_index=True)

    def read_json_to_dict(self, file_key: str) -> dict:
        """
        Reads a JSON file from the bucket and returns its content as a dictionary.

        Args:
            file_key (str): The key of the JSON file to read.

        Returns:
            dict: A dictionary containing the JSON file's data.
        """
        response = self.s3_client.get_object(Bucket=self.bucket_name , Prefix=self.data_dir , Key=file_key)
        json_content = json.loads(response['Body'].read().decode('utf-8'))
        return json_content