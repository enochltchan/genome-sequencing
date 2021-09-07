import json
import os
import time

import boto3
import botocore.client
from botocore.exceptions import ClientError

# Get ann_config configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'util_config.ini'))

# Send message to request queue
# Publish a notification message to the SNS topic
sns = boto3.client('sns', region_name=config['aws']['AwsRegionName'], config=botocore.client.Config(signature_version = 's3v4'))

while True:
    # Exceptions found in SNS publish() documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
    sns_publish = sns.publish(TopicArn=config['sns']['RequestsARN'], Message=json.dumps({'user_id': 'test'}), MessageStructure='string')

    # Send one message every 10 seconds = 6 messages every minute = 60 messages every 600 seconds > 50 messages every 600 seconds
    time.sleep(10)