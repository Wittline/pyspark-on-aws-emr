import logging
import boto3
from botocore.exceptions import ClientError


def create_bucket(bucket_name, folders, logger):

    try:
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': s3_resource.meta.client.meta.region_name
            }
        )
        bucket.wait_until_exists()
        logger.info("Created bucket %s.", bucket_name)
    except ClientError:
        logger.exception("Couldn't create bucket %s.", bucket_name)
        raise


    try:
        for folder in folders:
            s3_resource.put_object(Bucket= bucket_name, Body='', Key=folder + '/')
            logger.info("Folder created %s.", f'{bucket_name}/{folder}')
    except ClientError:
        logger.exception("Couldn't create folder %s.", folder)
        raise    

    return bucket
    

def setup_bucket(bucket_name, script_file_name, script_key, logger):

    try:
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': s3_resource.meta.client.meta.region_name
            }
        )
        bucket.wait_until_exists()
        logger.info("Created bucket %s.", bucket_name)
    except ClientError:
        logger.exception("Couldn't create bucket %s.", bucket_name)
        raise

    try:
        bucket.upload_file(script_file_name, script_key)
        logger.info(
            "Uploaded script %s to %s.", script_file_name,
            f'{bucket_name}/{script_key}')
    except ClientError:
        logger.exception("Couldn't upload %s to %s.", script_file_name, bucket_name)
        raise

    return bucket


def delete_bucket(bucket, logger):

    try:        
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(bucket)
        bucket.objects.delete()
        bucket.delete()
        logger.info("Emptied and removed bucket %s.", bucket)
    except ClientError:
        logger.exception("Couldn't remove bucket %s.", bucket)
        raise
