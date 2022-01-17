from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from loguru import logger
from tqdm import tqdm


class S3Url:
    """
    Get Bucket and Key from s3 url.

    >>> s = S3Url("s3://bucket/hello/world")
    >>> s.bucket
    'bucket'
    >>> s.key
    'hello/world'
    >>> s.url
    's3://bucket/hello/world'
    """

    def __init__(self, url):
        self._parsed = urlparse(url, allow_fragments=False)

    @property
    def bucket(self):
        return self._parsed.netloc

    @property
    def key(self):
        if self._parsed.query:
            return self._parsed.path.lstrip('/') + '?' + self._parsed.query
        else:
            return self._parsed.path.lstrip('/')

    def __str__(self):
        return self._parsed.geturl()


class S3Helper:
    """S3 Helper for downloading and uploading files to S3

    Usage examples:
        s3_helper = S3Helper()
        s3_helper.upload("input_sample.csv", "resarch-affinity-ml", "arcgate_input_sample.csv")
        df = s3_helper.download_object(
            "s3://research-affinity-ml/input_sample.csv",
            download_path="input_sample.csv"
        )
    """
    def __init__(self):
        self.s3 = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')

    def upload(self, file_name, bucket, object_name=None, extra_args=None) -> bool:
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        if not extra_args:
            extra_args = {'ServerSideEncryption': 'AES256'}

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = Path(file_name).name

        try:
            self.s3.upload_file(
                file_name, bucket, object_name,
                ExtraArgs=extra_args,
            )
            return True

        except ClientError as e:
            logger.error(e)
            return False

    def download_object(self, s3_url, download_path):
        """Download file from s3_url and persist it in download_path.

        :param s3_url: s3 object url
        :param download_path: Path to persist object
        """
        s3_url = S3Url(s3_url)

        logger.info(f'S3 Bucket: {s3_url.bucket}')
        logger.info(f'S3 Key: {s3_url.key}')
        res = self.s3.get_object(
            Bucket=s3_url.bucket, Key=s3_url.key
        )['Body'].read()
        with open(download_path, 'wb') as f:
            f.write(res)

    def download_directory(self, s3_url, download_path):
        """ Download all files in a directory from s3_url and persist them in download_path.

        :param s3_url: s3 object url
        :param download_path: Path to persist object
        """
        s3_url = S3Url(s3_url)

        logger.info(f'S3 Bucket: {s3_url.bucket}')
        logger.info(f'S3 Key: {s3_url.key}')

        bucket = self.s3_resource.Bucket(s3_url.bucket)

        for obj in tqdm(
            bucket.objects.filter(Prefix=s3_url.key),
            desc=f'Downloading from {s3_url.bucket}'
        ):
            # save to the same path
            dest_path = Path(download_path) / obj.key
            dest_path.parent.mkdir(exist_ok=True, parents=True)
            bucket.download_file(obj.key, str(dest_path))

    def iterate_bucket_items(self, s3_url: str) -> dict:
        """
        Generator that iterates over all objects in a given s3 bucket

        See http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.list_objects_v2
        for return data format

        :param bucket: Name of s3 bucket
        :return: Dict of metadata for an object
        """
        s3_url = S3Url(s3_url)
        logger.info(f'S3 Bucket: {s3_url.bucket}')
        logger.info(f'S3 Key: {s3_url.key}')

        paginator = self.s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=s3_url.bucket,
            Prefix=s3_url.key
        )

        for page in page_iterator:
            if page['KeyCount'] > 0:
                for item in page['Contents']:
                    yield item


    def load_object_memory(self, s3_url):
        """Download file from s3_url and persist it in download_path.

        :param s3_url: s3 object url
        :param download_path: Path to persist object
        """
        s3_url = S3Url(s3_url)

        try:
            res = self.s3.get_object(
                Bucket=s3_url.bucket, Key=s3_url.key
            )['Body'].read()
            return res

        except ClientError as e:
            logger.error(s3_url)
            logger.error(e)
            return None


    def upload_object_from_memory(self, data: bytes, s3_url_path: str, extra_args=None) -> bool:
        s3_url = S3Url(s3_url_path)

        try:
            self.s3.put_object(Body=data, Bucket=s3_url.bucket, Key=s3_url.key, ServerSideEncryption='AES256')
        except ClientError as e:
            logger.error(s3_url_path)
            logger.error(e)
            return False

        return True


    def truncate_s3_path(self, s3_url_path: str):
        s3_parsed =  S3Url(s3_url_path)
        s3_bucket = s3_parsed.bucket
        s3_key = s3_parsed.key

        parts = s3_key.split("/")
        destination = parts[:-1]
        destination = "/".join(destination) + "/"
        return s3_bucket, destination