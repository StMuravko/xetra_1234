"""Connector and methods accessing S3"""
import os
import logging

import boto3
from io import StringIO, BytesIO
import pandas as pd
from xetra.common.constants import S3FileTypes
from xetra.common.custom_exceptions import WrongFormatException


class S3BucketConnector():
    """
    class for interacting with s3 Buckets
    """

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str):
        """
        Constructor for S3 Bucket connector
        :param access_key: for accessing S3
        :param secret_key: key for accessing S3
        :param endpoint_url: end point url for accessing S3
        :param bucket: S3 Bucket
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                     aws_secret_access_key=os.environ[secret_key])
        self._s3 = self.session.resource(service_name='s3', endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)

    def list_filex_in_prefix(self, prefix: str):
        """
        Listing all files with a prefix on the S3 bucket
        :param prefix: prefix on the S3 bucket
        :return files: list of all the file names containing prefix in the key
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_csv_to_df(self, key: str, decoding='utf-8', sep=','):
        self._logger.info('Reading files %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        obj = self._bucket.Object(key=key).get().get('Body').read().decode(decoding)
        data_all = StringIO(obj)
        df = pd.read_csv(data_all, delimiter=sep)
        return df

    def write_df_to_s3(self, data_frame: pd.DataFrame, key: str, file_format: str):
        """

        :param data_frame:  Pandas DataFrame that should be written
        :param key: key of the saved file
        :param file_format: format of the saved file
        """
        if data_frame.empty:
            self._logger.info('The DataFrame is empty! File will not be written!')
            return None
        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        self._logger.info('The file format %s is not supported', file_format)
        raise WrongFormatException

    def __put_object(self, out_buffer: StringIO or BytesIO, key: str):
        self._logger.info('Writing file to %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True
