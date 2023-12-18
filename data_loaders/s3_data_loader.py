import boto3
import pandas as pd
from io import StringIO
from typing import List
import json

class S3DataLoader:
    """
    A class to load data from an S3 bucket.
    """

    def __init__(self, bucket_name: str, data_dir: str):
        """
        Initializes the S3DataLoader.

        Args:
            bucket_name (str): Name of the S3 bucket.
            data_dir (str): Directory in the bucket where files are stored.
        """
        assert isinstance(bucket_name, str) and bucket_name, "bucket_name must be a non-empty string"
        assert isinstance(data_dir, str) and data_dir, "data_dir must be a non-empty string"
        
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
        self.data_dir = data_dir

    def list_files(self) -> List[str]:
        """
        List all files in the specified S3 bucket.

        Returns:
            List[str]: A list of file names. Returns an empty list in case of an error.
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=self.data_dir)
            assert 'Contents' in response, "No files found in the bucket"
            return [item['Key'] for item in response['Contents']]
        except Exception as e:
            print(f"Error listing files: {e}")
            return []

    def read_all_txt_to_df(self) -> pd.DataFrame:
        """
        Reads all .txt files in the bucket into a single DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing the combined data from all .txt files.
                          Returns an empty DataFrame in case of an error.
        """
        try:
            all_files = self.list_files()
            assert all_files, "No files found in the list"
            txt_files = [f for f in all_files if f.endswith('.txt')]
            assert txt_files, "No .txt files found in the list"

            df_list = []
            for file in txt_files:
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file)
                df_list.append(pd.read_csv(StringIO(response['Body'].read().decode('utf-8'))))

            return pd.concat(df_list, ignore_index=True)
        except Exception as e:
            print(f"Error reading txt files to DataFrame: {e}")
            return pd.DataFrame()

    def read_json_to_dict(self, file_key: str) -> dict:
        """
        Reads a JSON file from the bucket and returns its content as a dictionary.

        Args:
            file_key (str): The key of the JSON file to read.

        Returns:
            dict: A dictionary containing the JSON file's data.
                  Returns an empty dictionary in case of an error.
        """
        try:
            assert isinstance(file_key, str) and file_key, "file_key must be a non-empty string"
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            json_content = json.loads(response['Body'].read().decode('utf-8'))
            return json_content
        except Exception as e:
            print(f"Error reading JSON file: {e}")
            return {}
