"""TestS3BucketConnectorsMethods"""

import os
import unittest

import boto3
from moto import mock_s3
import pandas as pd

from xetra.common.s3 import S3BucketConnector
from pandas import DataFrame
from io import StringIO, BytesIO
from xetra.common.custom_exceptions import WrongFormatException


class TestS3BucketConnectorMethods(unittest.TestCase):
    """
    Testing the S3BucketConnector
    """

    def setUp(self):
        """
        Setting up the enviroment
        :return:
        """
        # mocking s3 connection start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'
        # Create S3 access keys
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'
        # Creating bucket on the mocked s3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name, CreateBucketConfiguration={
            'LocationConstraint': 'eu-central-1'
        })
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        # Creating testing instance
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key, self.s3_secret_key,
                                                self.s3_endpoint_url, self.s3_bucket_name)

    def tearDown(self):
        """
        Exectunig after unittest
        :return:
        """
        # mocking s3 connection stop
        self.mock_s3.stop()
        pass

    def test_list_files_in_prefix_ok(self):
        # Expected results
        prefix_exp = 'prefix/'
        key1_exp = f'{prefix_exp}test1.csv'
        key2_exp = f'{prefix_exp}test2.csv'
        # Test init
        csv_content = """col1, col2
        valA,valB"""
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)
        # Method execution
        list_result = self.s3_bucket_conn.list_filex_in_prefix(prefix_exp)
        # Tests after method executions
        self.assertEqual(len(list_result), 2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)
        # Cleanup after tests
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key1_exp
                    },
                    {
                        'Key': key2_exp
                    }
                ]
            }
        )

    def test_list_files_in_prefix_wrong_prefix(self):
        # Expected results
        prefix_exp = 'no-prefix/'
        list_result = self.s3_bucket_conn.list_filex_in_prefix(prefix_exp)
        # Tests after method executions
        self.assertTrue(not list_result)

    def test_read_csv_to_df_ok(self):
        # Expected results
        key_exem1 = 'key1.csv'
        # Test init
        csv_content = """col1, col2
                valA,valB"""
        self.s3_bucket.put_object(Body=csv_content, Key=key_exem1)
        # Method execution
        df_result = self.s3_bucket_conn.read_csv_to_df(key_exem1)
        # Test after method executed
        self.assertIsInstance(df_result, DataFrame)
        self.assertEqual(df_result.shape[0], 1)
        self.assertEqual(df_result.shape[1], 2)
        # Clean after tesrt
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exem1
                    }
                ]
            }
        )

    def test_write_df_to_s3_empty(self):
        return_exp = None
        log_exp = 'The DataFrame is empty! File will not be written!'

        df_empty = pd.DataFrame()
        key = 'key.csv'
        format = 'csv'

        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_empty, key, file_format=format)
            self.assertIn(log_exp, logm.output[0])
        self.assertEqual(return_exp, result)

    def test_write_df_to_s3_csv(self):
        return_exp = True
        df_exp = pd.DataFrame([['a', 'b'], ['c', 'd']], columns=['col1', 'col2'])
        key_exp = 'test.csv'
        log_exp = f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'

        format = 'csv'

        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_exp, key_exp, file_format=format)
            self.assertIn(log_exp, logm.output[0])
        data = self.s3_bucket.Object(key_exp).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_result = pd.read_csv(out_buffer)
        self.assertEqual(return_exp, result)
        self.assertTrue(df_exp.equals(df_result))
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )

    def test_write_df_to_s3_parquet(self):
        return_exp = True
        df_exp = pd.DataFrame([['a', 'b'], ['c', 'd']], columns=['col1', 'col2'])
        key_exp = 'test.parquet'
        log_exp = f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'

        format = 'parquet'

        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_exp, key_exp, file_format=format)
            self.assertIn(log_exp, logm.output[0])
        data = self.s3_bucket.Object(key_exp).get().get('Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        self.assertEqual(return_exp, result)
        self.assertTrue(df_exp.equals(df_result))
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )

    def test_write_df_to_s3_wrong_format(self):
        df_exp = pd.DataFrame([['a', 'b'], ['c', 'd']], columns=['col1', 'col2'])
        key_exp = 'test.parquet'
        format = 'wrong_format'
        log_exp = f'The file format {format} is not supported'
        exception_exp = WrongFormatException

        with self.assertLogs() as logm:
            with self.assertRaises(exception_exp):
                self.s3_bucket_conn.write_df_to_s3(df_exp, key_exp, format)
            self.assertIn(log_exp, logm.output[0])


if __name__ == '__main__':
    unittest.main()
