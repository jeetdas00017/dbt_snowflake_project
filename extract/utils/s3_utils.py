import boto3
from botocore.config import Config

from extract.utils.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_ENDPOINT, S3_REGION
from extract.utils.logging_config import logger


def get_s3_client():
    logger.info("Creating S3 client")

    params = {
        "config": Config(signature_version="s3v4"),
    }
    if S3_REGION:
        params["region_name"] = S3_REGION

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        params["aws_access_key_id"] = AWS_ACCESS_KEY_ID
        params["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY

    if S3_ENDPOINT:
        params["endpoint_url"] = S3_ENDPOINT

    return boto3.client("s3", **params)


def upload_file_to_s3(local_path: str, bucket: str, s3_key: str) -> None:
    logger.info("Uploading local file %s to s3://%s/%s", local_path, bucket, s3_key)
    s3_client = get_s3_client()
    s3_client.upload_file(local_path, bucket, s3_key)
    logger.info("Upload completed for %s", s3_key)
