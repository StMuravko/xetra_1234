"""TestMetaProcessMethods"""

import os
import unittest
import boto3
import pandas as pd
from io import StringIO
from moto import mock_s3
from datetime import datetime, timedelta
from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess
from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFileEceptions


class TestMetaProcessMethods(unittest.TestCase):
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
        self.s3_bucket_meta = S3BucketConnector(self.s3_access_key, self.s3_secret_key,
                                                self.s3_endpoint_url, self.s3_bucket_name)
        self.dates = [(datetime.today().date() - timedelta(days=day)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                      for day in range(8)]

    def tearDown(self):
        """
        Exectunig after unittest
        :return:
        """
        # mocking s3 connection stop
        self.mock_s3.stop()

    def test_update_meta_file_no_meta_file(self):
        date_list_exp = ['2022-09-19', '2022-09-18']
        proc_date_list_exp = [datetime.today().date()] * 2

        meta_key = 'meta.csv'
        MetaProcess.update_meta_file(date_list_exp, meta_key, self.s3_bucket_meta)

        data = self.s3_bucket.Object(key=meta_key).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer, delimiter=',')
        date_list_result = list(df_meta_result[MetaProcessFormat.META_SOURCE_DATE_COL.value])
        proc_date_list_result = list(
            pd.to_datetime(df_meta_result[MetaProcessFormat.META_PROCESS_COL.value]).dt.date
        )
        self.assertEqual(date_list_exp, date_list_result)
        self.assertEqual(proc_date_list_exp, proc_date_list_result)
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_update_meta_file_empy_date_list(self):
        return_exp = True
        log_exp = 'The DataFrame is empty! File will not be written!'

        date_list = []
        meta_key = 'test.csv'

        with self.assertLogs() as logm:
            result = MetaProcess.update_meta_file(date_list, meta_key, self.s3_bucket_meta)
            self.assertIn(log_exp, logm.output[1])
        self.assertEqual(return_exp, result)

    def test_update_meta_file_meta_file_ok(self):
        date_list_old = ['2022-09-16', '2022-09-17']
        date_list_new = ['2022-09-18', '2022-09-19']
        date_list_exp = date_list_old + date_list_new
        proc_date_list_exp = [datetime.today().date()] * 4

        meta_key = 'test.csv'
        meta_content = (
            f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},'
            f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{date_list_old[0]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n'
            f'{date_list_old[1]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n'
        )

        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        MetaProcess.update_meta_file(date_list_new, meta_key, self.s3_bucket_meta)

        data = self.s3_bucket.Object(key=meta_key).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer, delimiter=',')
        date_list_result = list(df_meta_result[
                                    MetaProcessFormat.META_SOURCE_DATE_COL.value
                                ])
        proc_date_list_result = list(pd.to_datetime(df_meta_result[
                                                        MetaProcessFormat.META_PROCESS_COL.value
                                                    ]).dt.date
                                     )
        self.assertEqual(date_list_exp, date_list_result)
        self.assertEqual(proc_date_list_exp, proc_date_list_result)
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_update_meta_file_meta_file_wrong(self):
        date_list_old = ['2022-09-16', '2022-09-17']
        date_list_new = ['2022-09-18', '2022-09-19']

        meta_key = 'test.csv'
        meta_content = (
            f'wrong_column, {MetaProcessFormat.META_SOURCE_DATE_COL.value},'
            f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{date_list_old[0]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n'
            f'{date_list_old[1]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n'
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        with self.assertRaises(WrongMetaFileEceptions):
            MetaProcess.update_meta_file(date_list_new, meta_key, self.s3_bucket_meta)
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_return_date_list_no_meta_file(self):
        date_list_exp = [
            (datetime.today().date() - timedelta(days=day)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
            for day in range(4)
        ]
        min_date_exp = (
                datetime.today().date() - timedelta(days=2)
        ).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        first_date = min_date_exp
        meta_key = 'meta.csv'
        min_date_return, date_list_return = MetaProcess.return_date_list(first_date, meta_key, self.s3_bucket_meta)
        self.assertEqual(set(date_list_exp), set(date_list_return))
        self.assertEqual(min_date_exp, min_date_return)

    def test_return_date_list_meta_file_ok(self):
        min_date_exp = [
            (
                    datetime.today().date() - timedelta(days=1)
            ).strftime(MetaProcessFormat.META_DATE_FORMAT.value),
            (
                    datetime.today().date() - timedelta(days=2)
            ).strftime(MetaProcessFormat.META_DATE_FORMAT.value),
            (
                    datetime.today().date() - timedelta(days=7)
            ).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        ]
        date_list_exp = [
            [
                (datetime.today().date() - timedelta(days=day)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                for day in range(3)
            ],
            [
                (datetime.today().date() - timedelta(days=day)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                for day in range(4)
            ],
            [
                (datetime.today().date() - timedelta(days=day)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                for day in range(9)
            ]
        ]
        meta_key = 'meta.csv'
        meta_content = (
            f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},'
            f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{self.dates[3]},{self.dates[0]}\n'
            f'{self.dates[4]},{self.dates[0]}'

        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date_list = [
            self.dates[1],
            self.dates[4],
            self.dates[7]
        ]

        for count, first_date in enumerate(first_date_list):
            min_date_return, date_list_return = MetaProcess.return_date_list(first_date, meta_key, self.s3_bucket_meta)
            self.assertEqual(set(date_list_exp[count]), set(date_list_return))
            self.assertEqual(min_date_exp[count], min_date_return)
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_returtn_date_list_meta_file_wrong(self):
        meta_key = 'meta.csv'
        meta_content = (
            f'wrong_column,{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{self.dates[3]},{self.dates[0]}\n'
            f'{self.dates[4]},{self.dates[0]}'
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date = self.dates[1]

        with self.assertRaises(KeyError):
            MetaProcess.return_date_list(first_date, meta_key, self.s3_bucket_meta)
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_return_date_list_empty_dates_list(self):
        min_date_exp = '2200-01-01'
        date_list_exp = []
        meta_key = 'meta.csv'
        meta_content = (
            f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},'
            f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{self.dates[0]},{self.dates[0]}\n'
            f'{self.dates[1]},{self.dates[0]}'
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date = self.dates[0]

        min_dare_returne, date_list_return = MetaProcess.return_date_list(first_date, meta_key, self.s3_bucket_meta)

        self.assertEqual(date_list_exp, date_list_return)
        self.assertEqual(min_date_exp, min_dare_returne)
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )


if __name__ == '__main__':
    unittest.main()
