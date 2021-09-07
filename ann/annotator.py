import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import botocore.client
import json
import os
import re
import subprocess

# Get ann_config configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))

# Connect to SQS and get the message queue
sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'], config=botocore.client.Config(signature_version = 's3v4'))
queue = sqs.Queue(config['sqs']['RequestsURL'])

# Set visibility timeout to 15 minutes because this is around the amount of time it takes for a t2.nano to process the largest file, premium_2.vcf
# set_attributes() from boto documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.set_attributes
# Visibility timeout information from AWS documentation: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
queue.set_attributes(Attributes={'VisibilityTimeout': '1000'})

# Poll the message queue in a loop 
while True:

    # Attempt to read a message from the queue using long polling
    # Receive one message at a time as a best practice, so that in the case where there were other instances picking up messages each message will only get picked up once
    # Use of receive_messages() from boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
    messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=5)

    # If message read, extract job parameters from the message body as before
    if len(messages) > 0:
        for message in messages:
            sqs_body = json.loads(message.body)
            data = json.loads(sqs_body['Message'])

            # Include below the same code you used in prior homework
            # Get the input file S3 object and copy it to a local file
            # Use a local directory structure that makes it easy to organize
            # multiple running annotation jobs
            if not isinstance(data, dict):
                print('Error: Data is not in correct format (dictionary)')

            try:
                bucket_name = data['s3_inputs_bucket']
            except:
                print('Error: bucket name does not exist in data')
            try:
                key = data['s3_key_input_file']
            except:
                print('Error: key does not exist in data')
            try:
                job_id = data['job_id']
            except:
                print('Error: job_id does not exist in data')
            try:
                input_file = data['input_file_name']
            except:
                print('Error: file name does not exist in data')
            subfolder = re.split('~',key)[0].replace('/','~')

            if input_file.find('.vcf') < 0:
                print('Error: Annotation file is not in .vcf file format')

            s3 = boto3.resource('s3', region_name=config['aws']['AwsRegionName'], config=botocore.client.Config(signature_version = 's3v4'))
            # Accessing bucket and existence check from boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/migrations3.html
            try:
                s3.meta.client.head_bucket(Bucket=bucket_name)
            except ClientError as e:
                error = e.response['Error']
                if error['Code'] == '404':
                    print(f"Error: {bucket_name} does not exist: {error['Message']}")

            # Use of os.path.expanduser() from: https://stackoverflow.com/questions/40662821/tilde-isnt-working-in-subprocess-popen
            current_filepath = os.path.abspath(os.path.dirname(__file__))

            # Check that folders that will house the annotated file exist
            if not os.path.exists(f'{current_filepath}/jobs'):
                try:
                    # Make jobs subfolder within annotation to store all jobs
                    # Use of os.mkdir() from: https://www.geeksforgeeks.org/create-a-directory-in-python/
                    os.mkdir(f'{current_filepath}/jobs')
                except:
                    print('Failed to create subfolder to store annotation jobs')
            if not os.path.exists(f'{current_filepath}/jobs/{subfolder}'):
                try:
                    # Create subfolder with unique id
                    os.mkdir(f'{current_filepath}/jobs/{subfolder}')
                except:
                    print('Failed to create unique subfolder to store annotation job')

            # Download file from S3 bucket
            try:
                # download_file() from boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.download_file
                s3.meta.client.download_file(Bucket=bucket_name, Key=key, Filename=f'{current_filepath}/jobs/{subfolder}/{input_file}')
            except ClientError as e:
                error = e.response['Error']
                print(f"Error: Unable to download file: {error['Message']}")

            if not os.path.exists(f'{current_filepath}/run.py'):
                print('Annotator file does not exist')

            # Launch annotation job as a background process
            try:
                args = ['python', f'{current_filepath}/run.py', f'{current_filepath}/jobs/{subfolder}/{input_file}']
                ann_process = subprocess.Popen(args)
            except:
                print('Annotator failed to run')

            # Delete the message from the queue, if job was successfully submitted
            try:
                message.delete()
            except:
                print('Message failed to be deleted')

            # Update job status in DynamoDB table to RUNNING
            dynamo = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'], config=botocore.client.Config(signature_version = 's3v4'))

            # Exceptions found in dynamoDB boto documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
            try:
                dynamo_table = dynamo.Table(config['dynamo']['TableName'])
            except ClientError as e:
                error = e.response['Error']
                print(f"Error: Unable to access DynamoDB table: {error['Message']}")

            try:
                # Use of update_item() from:
                #   - boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.update_item
                #   - example 1: https://www.programcreek.com/python/example/103724/boto3.dynamodb.conditions.Attr
                #   - example 2: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
                dynamo_table.update_item(Key={'job_id': job_id},
                                         UpdateExpression='SET job_status = :r',
                                         ExpressionAttributeValues={':r': 'RUNNING'},
                                         ConditionExpression=Attr('job_status').eq('PENDING'))
            except ClientError as e:
                error = e.response['Error']
                print(f"Error: Unable to update item: {error['Message']}")