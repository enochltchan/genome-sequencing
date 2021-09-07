# thaw.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import botocore.client
import json
import re

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('thaw_config.ini')

# Add utility code here
# Connect to SQS and get the message queue
sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'], config=botocore.client.Config(signature_version = 's3v4'))
queue = sqs.Queue(config['sqs']['ThawURL'])

# Set visibility timeout to 15 seconds, an arbitrarily low number
# set_attributes() from boto documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.set_attributes
# Visibility timeout information from AWS documentation: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
queue.set_attributes(Attributes={'VisibilityTimeout': '15'})

# Poll the message queue in a loop 
while True:

    # Attempt to read a message from the queue using long polling
    # Receive one message at a time as a best practice, so that in the case where there were other instances picking up messages each message will only get picked up once
    # Use of receive_messages() from boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
    messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=5)

    # If message read, extract job parameters from the message body
    if len(messages) > 0:
        for message in messages:
            sqs_body = json.loads(message.body)
            data = json.loads(sqs_body['Message'])

            if not isinstance(data, dict):
                print('Error: Data is not in correct format (dictionary)')

            try:
                glacier_job_id = data['JobId']
            except:
                print('Error: JobId does not exist in data')
            try:
                archive_id = data['ArchiveId']
            except:
                print('Error: ArchiveId does not exist in data')
            try:
                user_id = data['JobDescription']
            except:
                print('Error: JobDescription does not exist in data')

            # Download restored Glacier archive
            glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'], config=botocore.client.Config(signature_version = 's3v4'))

            # get_job_output() from: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.get_job_output
            try:
                archive = glacier.get_job_output(vaultName=config['glacier']['VaultName'], jobId=glacier_job_id)
            except ClientError as e:
                error = e.response['Error']
                print(f"Unable to get job output from Glacier: {error['Message']}. Now using standard restoration.")
            
            # Access job info from DynamoDB database
            dynamo = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'], config=botocore.client.Config(signature_version = 's3v4'))

            # Exceptions found in dynamoDB boto documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
            try:
                dynamo_table = dynamo.Table(config['dynamo']['TableName'])
            except ClientError as e:
                error = e.response['Error']
                print(f"Unable to access DynamoDB table: {error['Message']}")

            # Use of query() from boto documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
            try:
                response = dynamo_table.query(IndexName='user_id_index', KeyConditionExpression=Key('user_id').eq(user_id),
                                                FilterExpression='results_file_archive_id = :r', ExpressionAttributeValues={':r': archive_id})
            except ClientError as e:
                error = e.response['Error']
                print(f"Unable to query DynamoDB table: {error['Message']}")

            archive_contents = archive['body'].read()
            annotation = response['Items'][0]
            s3_key_log_file = annotation['s3_key_log_file']
            s3_key_result_file = re.sub('.vcf.count.log', '.annot.vcf', s3_key_log_file)
            bucket_name = config['s3']['ResultsBucketName']

            # Upload data to s3
            s3 = boto3.resource('s3', region_name=config['aws']['AwsRegionName'], config=botocore.client.Config(signature_version = 's3v4'))

            # Accessing bucket and existence check from boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/migrations3.html
            try:
                s3.meta.client.head_bucket(Bucket=bucket_name)
            except ClientError as e:
                error = e.response['Error']
                if error['Code'] == '404':
                    print(f"Error: {bucket_name} does not exist: {error['Message']}")

            try:
                # put_object() from boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object
                response = s3.meta.client.put_object(Bucket=bucket_name, Key=s3_key_result_file, Body=archive_contents)
            except ClientError as e:
                error = e.response['Error']
                print(f"Unable to put object into s3 bucket: {error['Message']}")

            # Update dynamo
            try:
                # Use of update_item() from:
                #   - boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.update_item
                #   - example 1: https://www.programcreek.com/python/example/103724/boto3.dynamodb.conditions.Attr
                #   - example 2: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
                dynamo_table.update_item(Key={'job_id': annotation['job_id']},
                                         UpdateExpression='SET s3_key_result_file = :r REMOVE results_file_archive_id',
                                         ExpressionAttributeValues={':r': s3_key_result_file})
            except ClientError as e:
                error = e.response['Error']
                print(f"Error: Unable to update item in database: {error['Message']}")

            # Delete the message from the queue
            try:
                message.delete()
            except:
                print('Message failed to be deleted')

### EOF