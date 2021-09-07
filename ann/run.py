import sys
import time
import driver
import boto3
from botocore.exceptions import ClientError
import botocore.client
import shutil
import math
import os
import json

# Get ann_config configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))

"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")

if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')
        '''
        Three objectives:
            - Upload the results file to gas-results
            - Upload the log file to gas-results
            - Clean up (delete) local job files
            - Update the job item in DynamoDB
        '''
        bucket_name = config['s3']['ResultsBucketName']
        log_file_local = f'{sys.argv[1]}.count.log'
        results_file_local = sys.argv[1].replace('.vcf', '.annot.vcf')

        filepath_split = sys.argv[1].split('/')
        folder = sys.argv[1].split(filepath_split[-1])[0]
        subfolder = filepath_split[-2]
        log_file_key = subfolder.replace('~','/')+'~'+filepath_split[-1]+'.count.log'
        results_file_key = subfolder.replace('~','/')+'~'+filepath_split[-1].replace('.vcf', '.annot.vcf')
        job_id = subfolder.split('~')[-1]
        user_id = subfolder.split('~')[-2]

        s3 = boto3.resource('s3', region_name=config['aws']['AwsRegionName'], config=botocore.client.Config(signature_version = 's3v4'))
        # Accessing bucket and existence check from boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/migrations3.html
        try:
            s3.meta.client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            error = e.response['Error']
            if error['Code'] == '404':
                    print(f"Error: {bucket_name} does not exist: {error['Message']}")

        # Upload log file
        try:
            # upload_file() from boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_file
            s3.meta.client.upload_file(Filename=log_file_local, Bucket=bucket_name, Key=log_file_key)
        except ClientError as e:
            error = e.response['Error']
            print(f"Error: Unable to upload log file: {error['Message']}")

        # Upload results file
        try:
            # upload_file() from boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_file
            s3.meta.client.upload_file(Filename=results_file_local, Bucket=bucket_name, Key=results_file_key)
        except ClientError as e:
            error = e.response['Error']
            print(f"Error: Unable to upload results file: {error['Message']}")

        # Delete entire folder of local data
        try:
            shutil.rmtree(folder)
        except:
            print('Error: failed to remove local files')

        # Update the item in DynamoDB
        dynamo = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])

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
            # using time.time() to get epoch time from documentation: https://docs.python.org/3/library/time.html#time.time
            # use math.floor() to round down the time to the nearest integer second
            # e.g. if a user submitted a job in the 6.7th second, then she submitted a job sometime in the 6th second.
            dynamo_table.update_item(Key={'job_id': job_id},
                                     UpdateExpression='SET job_status = :c, s3_results_bucket = :g, s3_key_result_file = :r, s3_key_log_file = :l, complete_time = :t',
                                     ExpressionAttributeValues={':c': 'COMPLETED',
                                                                ':g': bucket_name,
                                                                ':r': results_file_key,
                                                                ':l': log_file_key,
                                                                ':t': math.floor(time.time())})
        except ClientError as e:
            error = e.response['Error']
            print(f"Error: Unable to update item: {error['Message']}")

        # Send message to results queue for notify.py to pick up and send email to user
        # Just need to send job_id and user_id
        data = {'job_id': job_id, 'user_id': user_id}

        # Publish a notification message to the SNS topic
        sns = boto3.client('sns', region_name=config['aws']['AwsRegionName'], config=botocore.client.Config(signature_version = 's3v4'))

        # Exceptions found in SNS publish() documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
        try:
            sns_publish = sns.publish(TopicArn=config['sns']['ResultsARN'], Message=json.dumps(data), MessageStructure='string')
        except ClientError as e:
            error = e.response['Error']
            print(f"Error: Unable to publish message to SNS: {error['Message']}")

        # Send message to archives queue for archive.py to pick up and archive to glacier
        # Just need to send job_id and user_id, same data as above
        # Exceptions found in SNS publish() documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
        try:
            sns_publish = sns.publish(TopicArn=config['sns']['ArchivesARN'], Message=json.dumps(data), MessageStructure='string')
        except ClientError as e:
            error = e.response['Error']
            print(f"Error: Unable to publish message to SNS: {error['Message']}")

    else:
        print("A valid .vcf file must be provided as input to this program.")

### EOF