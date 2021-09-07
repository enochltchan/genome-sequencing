# notify.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
import os
import sys
import boto3
from botocore.exceptions import ClientError
import botocore.client
import json

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('notify_config.ini')

# Connect to SQS and get the message queue
sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'], config=botocore.client.Config(signature_version = 's3v4'))
queue = sqs.Queue(config['sqs']['ResultsURL'])

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

            # Send email
            body = f"Job ID {job_id} has completed. View the log and download the results file here: {config['web']['AnnDetailsURL']}/{job_id}"
            try:
                helpers.send_email_ses(recipients=user_email, subject='Job completed', body=body)
            except:
                print('Error: email failed to send.')

            # Delete the message from the queue, if job was successfully submitted
            try:
                message.delete()
            except:
                print('Message failed to be deleted')

### EOF