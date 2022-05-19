""" Connector  and methods accessing s3"""
import os
import logging
import boto3

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

    def read_csv_to_df(bucket, key, decoding = 'utf-8', sep = ','):
        csv_obj = bucket.Object(key=key).get().get('Body').read().decode(decoding)
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=sep)
        return df

    def write_df_to_s3(bucket, df, key):
        out_buffer = BytesIO()
        df.to_parquet(out_buffer, index=False)
        bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True

    def list_files_in_prefix(self, prefix: str):
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files