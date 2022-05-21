""" Connector  and methods accessing s3"""
import os
import logging
from io import StringIO, BytesIO
import boto3
import pandas as pd

from xetra.common.constants import S3FileTypes
from xetra.common.custom_exceptions import WrongFormatException


class S3BucketConnector():
    """
    class for interactin with s3 bucket
    """
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str):
        """
        Constructor for s3 bucket connector
        :param access_key:
        :param secret_key:
        :param endpoint_url:
        :param bucket:
        """
        self._logger = logging.getLogger(__name__)

        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id = os.environ[access_key],
                                aws_secret_access_key= os.environ[secret_key])
        self._s3 = self.session.resource(service_name='s3', endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)

    def read_csv_to_df(self, key, decoding = 'utf-8', sep = ','):
        self._logger.info('Reading file %s/%s/%s', self.endpoint_url,self._bucket.name, key)
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(decoding)
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=sep)
        return df

    def write_df_to_s3(self, data_frame: pd.DataFrame, key: str, file_format: str):
        """
        writing a Pandas DataFrame to S3
        supported formats: .csv, .parquet

        :data_frame: Pandas DataFrame that should be written
        :key: target key of the saved file
        :file_format: format of the saved file
        """
        if data_frame.empty:
            self._logger.info('The dataframe is empty! No file will be written!')
            return None
        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        self._logger.info('The file format %s is not '
                          'supported to be written to s3!', file_format)
        raise WrongFormatException

    def __put_object(self, out_buffer: StringIO or BytesIO, key: str):
        """
        Helper function for self.write_df_to_s3()

        :out_buffer: StringIO | BytesIO that should be written
        :key: target key of the saved file
        """
        self._logger.info('Writing file to %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True

    def list_files_in_prefix(self, prefix: str):
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files