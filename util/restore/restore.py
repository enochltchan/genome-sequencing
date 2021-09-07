# restore.py
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
config.read('restore_config.ini')

# Connect to SQS and get the message queue
sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'], config=botocore.client.Config(signature_version = 's3v4'))
queue = sqs.Queue(config['sqs']['RestoreURL'])

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
                user_id = data['user_id']
            except:
                print('Error: user_id does not exist in data')

            # Obtain user info
            try:
                profile = helpers.get_user_profile(id=user_id)
            except:
                print('Error: unable to get user profile')
            user_role = profile['role']

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
                response = dynamo_table.query(IndexName='user_id_index', KeyConditionExpression=Key('user_id').eq(user_id))
            except ClientError as e:
                error = e.response['Error']
                print(f"Unable to query DynamoDB table: {error['Message']}")

            for annotation in response['Items']:
                if 'results_file_archive_id' not in annotation:
                    # The annotation has not yet been archived.
                    # In this case, there is potentially an archival message in flight for this particular annotation.
                    # The archive.py file will receive this message check the user's role.
                    # Seeing that the user is now premium, it will delete the archive message without doing any processing/archiving.
                    continue
                else:
                    # The annotation has been archived. Request expedited restoration.
                    glacier_id = annotation['results_file_archive_id']

                    # Upload archive to Glacier
                    glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'], config=botocore.client.Config(signature_version = 's3v4'))

                    # initiate_job() from: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job
                    # Transmit user_id info through the JobDescription field.
                    try:
                        # Expedited restoration
                        restoration_response = glacier.initiate_job(
                            vaultName=config['glacier']['VaultName'],
                            jobParameters={'Type': 'archive-retrieval',
                                            'ArchiveId': glacier_id,
                                            'SNSTopic': config['sns']['ThawARN'],
                                            'Tier': 'Expedited',
                                            'Description': user_id})
                    except ClientError as e:
                        error = e.response['Error']
                        print(f"Unable to initiate expedited restoration: {error['Message']}")
                        try:
                            # Standard restoration
                            restoration_response = glacier.initiate_job(
                                vaultName=config['glacier']['VaultName'],
                                jobParameters={'Type': 'archive-retrieval',
                                                'ArchiveId': glacier_id,
                                                'SNSTopic': config['sns']['ThawARN'],
                                                'Tier': 'Standard',
                                                'Description': user_id})
                        except ClientError as e:
                            error = e.response['Error']
                            print(f"Error: Unable to initiate standard restoration: {error['Message']}")

            # Delete the message from the queue
            try:
                message.delete()
            except:
                print('Message failed to be deleted')

### EOF