# archive.py
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

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('archive_config.ini')

# Add utility code here

# Connect to SQS and get the message queue
sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'], config=botocore.client.Config(signature_version = 's3v4'))
queue = sqs.Queue(config['sqs']['ArchivesURL'])

# Set DelaySeconds to 300 seconds (i.e. 5 minutes) because we submit messages to the queue upon completion of the annotation job. We only want to archive the results 5 minutes after this completion.
# Set visibility timeout to 15 seconds, an arbitrarily low number
# set_attributes() from boto documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.set_attributes
# Visibility timeout information from AWS documentation: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
queue.set_attributes(Attributes={'DelaySeconds': '300', 'VisibilityTimeout': '15'})

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
                job_id = data['job_id']
            except:
                print('Error: job_id does not exist in data')
            try:
                user_id = data['user_id']
            except:
                print('Error: user_id does not exist in data')

            # Obtain user name and email
            try:
                profile = helpers.get_user_profile(id=user_id)
            except:
                print('Error: unable to get user profile')
            user_name = profile['name']
            user_email = profile['email']
            user_role = profile['role']

            if user_role == 'free_user':
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
                    response = dynamo_table.query(KeyConditionExpression=Key('job_id').eq(job_id))
                except ClientError as e:
                    error = e.response['Error']
                    print(f"Unable to query DynamoDB table: {error['Message']}")

                job = response['Items'][0]
                bucket_name = config['s3']['ResultsBucketName']
                key = job['s3_key_result_file']

                # Get object from s3 to Glacier
                s3 = boto3.resource('s3', region_name=config['aws']['AwsRegionName'], config=botocore.client.Config(signature_version = 's3v4'))

                # Accessing bucket and existence check from boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/migrations3.html
                try:
                    s3.meta.client.head_bucket(Bucket=bucket_name)
                except ClientError as e:
                    error = e.response['Error']
                    if error['Code'] == '404':
                        print(f"Error: {bucket_name} does not exist: {error['Message']}")

                # get_object() from boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
                try:
                    s3_object = s3.meta.client.get_object(Bucket=bucket_name, Key=key)
                except ClientError as e:
                    error = e.response['Error']
                    print(f"Error: Unable to get object: {error['Message']}")

                s3_object_content = s3_object['Body'].read()

                # Upload archive to Glacier
                glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'], config=botocore.client.Config(signature_version = 's3v4'))

                # upload_archive() from: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.upload_archive
                try:
                    glacier_upload_response = glacier.upload_archive(vaultName=config['glacier']['VaultName'], body=s3_object_content)
                except ClientError as e:
                    error = e.response['Error']
                    print(f"Error: Unable to get object: {error['Message']}")

                glacier_id = glacier_upload_response['archiveId']

                # Persist Glacier ID in DynamoDB database and remove s3 result key
                try:
                    # Use of update_item() from:
                    #   - boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.update_item
                    #   - example 1: https://www.programcreek.com/python/example/103724/boto3.dynamodb.conditions.Attr
                    #   - example 2: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
                    dynamo_table.update_item(Key={'job_id': job_id},
                                             UpdateExpression='SET results_file_archive_id = :r REMOVE s3_key_result_file',
                                             ExpressionAttributeValues={':r': glacier_id})
                except ClientError as e:
                    error = e.response['Error']
                    print(f"Error: Unable to update item in database: {error['Message']}")

                # Delete object from s3 bucket
                try:
                    s3.meta.client.delete_object(Bucket=bucket_name, Key=job['s3_key_result_file'])
                except ClientError as e:
                    error = e.response['Error']
                    print(f"Error: Unable to delete object from {bucket_name}: {error['Message']}")
                
            # Delete the message from the queue
            try:
                message.delete()
            except:
                print('Message failed to be deleted')

### EOF