# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##

__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template, 
    request, session, url_for, jsonify)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile

import re
import math
import time

# Helper function
def error_response(code, message):
    '''
    Generate an error response to client requests
    '''
    response = dict()
    response['code'] = code
    response['status'] = 'error'
    response['message'] = message
    return jsonify(response)

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
    # Create a session client to the S3 service
    s3 = boto3.client('s3', 
        region_name=app.config['AWS_REGION_NAME'],
        config=Config(signature_version='s3v4'))

    bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
    user_id = session['primary_identity']

    # Generate unique ID to be used as S3 key (name)
    key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
        str(uuid.uuid4()) + '~${filename}'

    # Create the redirect URL
    redirect_url = str(request.url) + '/job'

    # Define policy fields/conditions
    encryption = app.config['AWS_S3_ENCRYPTION']
    acl = app.config['AWS_S3_ACL']
    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl}
    ]

    # Generate the presigned POST call
    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=bucket_name, 
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)
    
    # Render the upload form which will parse/submit the presigned POST
    return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():
    # Extract user ID from session
    user_id = session.get('primary_identity')

    # Get bucket name, key, and job ID from the S3 redirect URL
    bucket_name = str(request.args.get('bucket'))
    s3_key = str(request.args.get('key'))

    # Extract the job ID from the S3 key
    # re.split() method from: https://www.geeksforgeeks.org/python-split-multiple-characters-from-string/
    job_id = re.split('/|~',s3_key)[2]
    input_file = re.split('~',s3_key)[1]

    if input_file.find('.vcf') < 0:
        return error_response(400, f'Annotation file is not in .vcf file format')

    # Persist job to database
    # Create a job item and persist it to the annotations database
    data = {'job_id': job_id, 
           'user_id': user_id,
           'input_file_name': input_file,
           's3_inputs_bucket': bucket_name,
           's3_key_input_file': s3_key,
           # using time.time() to get epoch time from documentation: https://docs.python.org/3/library/time.html#time.time
           # use math.floor() to round down the time to the nearest integer second
           # e.g. if a user submitted a job in the 6.7th second, then she submitted a job sometime in the 6th second.
           'submit_time': math.floor(time.time()),
           'job_status': 'PENDING'}

    dynamo = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'], config=Config(signature_version = 's3v4'))

    # Exceptions found in dynamoDB boto documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
    try:
        dynamo_table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    except ClientError as e:
        error = e.response['Error']
        return error_response(500, f"Unable to access DynamoDB table: {error['Message']}")

    # Exceptions found in dynamoDB put_item() documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.put_item 
    try:
        dynamo_table.put_item(Item=data)
    except ClientError as e:
        error = e.response['Error']
        return error_response(500, f"Unable to enter item into DynamoDB table: {error['Message']}")

    # Send message to request queue
    # Publish a notification message to the SNS topic
    sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'], config=Config(signature_version = 's3v4'))

    # Exceptions found in SNS publish() documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
    try:
        sns_publish = sns.publish(TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'], Message=json.dumps(data), MessageStructure='string')
    except ClientError as e:
        error = e.response['Error']
        return error_response(500, f"Unable to publish message to request SNS: {error['Message']}")

    return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
    # Get list of annotations to display
    user_id = session.get('primary_identity')

    dynamo = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'], config=Config(signature_version = 's3v4'))

    # Exceptions found in dynamoDB boto documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
    try:
        dynamo_table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    except ClientError as e:
        error = e.response['Error']
        return error_response(500, f"Unable to access DynamoDB table: {error['Message']}")

    # Use of query() from boto documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
    try:
        response = dynamo_table.query(IndexName='user_id_index', KeyConditionExpression=Key('user_id').eq(user_id))
    except ClientError as e:
        error = e.response['Error']
        return error_response(500, f"Unable to query DynamoDB table: {error['Message']}")

    # Use of fromtimestamp() from: https://stackoverflow.com/questions/1697815/how-do-you-convert-a-time-struct-time-object-into-a-datetime-object
    # Use of format from: https://pymotw.com/2/datetime/
    annotations = []
    date_format = '%Y-%m-%d %H:%M'
    for annotation in response['Items']:
        annotation['submit_time'] = (datetime.fromtimestamp(annotation['submit_time'])).strftime(date_format)
        annotations.append(annotation)

    return render_template('annotations.html', annotations=annotations)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
    user_id = session.get('primary_identity')
    profile = get_profile(identity_id=user_id)
    user_role = profile.role

    dynamo = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'], config=Config(signature_version = 's3v4'))

    # Exceptions found in dynamoDB boto documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
    try:
        dynamo_table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    except ClientError as e:
        error = e.response['Error']
        return error_response(500, f"Unable to access DynamoDB table: {error['Message']}")

    # Use of query() from boto documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
    try:
        response = dynamo_table.query(KeyConditionExpression=Key('job_id').eq(id))
    except ClientError as e:
        error = e.response['Error']
        return error_response(500, f"Unable to query DynamoDB table: {error['Message']}")

    data = response['Items'][0]
    if data['user_id'] != user_id:
        return error_response(400, 'Not authorized to view this job')

    # Use of fromtimestamp() from: https://stackoverflow.com/questions/1697815/how-do-you-convert-a-time-struct-time-object-into-a-datetime-object
    # Use of format from: https://pymotw.com/2/datetime/
    date_format = '%Y-%m-%d %H:%M'

    annotation = {
        'job_id': data['job_id'],
        'submit_time': (datetime.fromtimestamp(data['submit_time'])).strftime(date_format),
        'input_file_name': data['input_file_name'],
        'job_status': data['job_status']
    }

    # complete_time
    if 'complete_time' in data:
        annotation['complete_time'] = (datetime.fromtimestamp(data['complete_time'])).strftime(date_format)

    # free_access_expired
    if (user_role == 'free_user') and ('complete_time' in data):
        # Check if 5 min have passed since annotation completion
        # Use of time difference from: https://stackoverflow.com/questions/43305577/python-calculate-the-difference-between-two-datetime-time-objects/43308104
        time_diff = datetime.now() - datetime.fromtimestamp(data['complete_time'])
        free_access_expired = time_diff.total_seconds()/60 > 5
    else:
        free_access_expired = False

    # restore_message
    if (user_role == 'premium_user') and ('s3_key_result_file' not in data):
        annotation['restore_message'] = 'Your result file is currently being restored and will be available for download soon.'

    # Generate presigned URLs
    s3 = boto3.resource('s3', region_name=app.config['AWS_REGION_NAME'], config=Config(signature_version = 's3v4'))

    # Generate URL to download results file
    inputs_bucket_name = data['s3_inputs_bucket']
    inputs_file_key = data['s3_key_input_file']

    # Accessing bucket and existence check from boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/migrations3.html
    try:
        s3.meta.client.head_bucket(Bucket=inputs_bucket_name)
    except ClientError as e:
        error = e.response['Error']
        if error['Code'] == '404':
            return error_response(500, f"{inputs_bucket_name} does not exist: {error['Message']}")

    # Use of generate_presigned_url() from: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
    try:
        presigned_url_input = s3.meta.client.generate_presigned_url('get_object', Params={'Bucket': inputs_bucket_name, 'Key': inputs_file_key}, ExpiresIn=60*2)
    except ClientError as e:
        error = e.response['Error']
        return error_response(500, f"Unable to generate presigned URL (input file): {error['Message']}")
    annotation['input_file_url'] = presigned_url_input

    # Generate URL to download results file
    if 's3_key_result_file' in data:
        results_bucket_name = data['s3_results_bucket']
        results_file_key = data['s3_key_result_file']

        # Accessing bucket and existence check from boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/migrations3.html
        try:
            s3.meta.client.head_bucket(Bucket=results_bucket_name)
        except ClientError as e:
            error = e.response['Error']
            if error['Code'] == '404':
                return error_response(500, f"{results_bucket_name} does not exist: {error['Message']}")

        # Use of generate_presigned_url() from: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
        try:
            presigned_url_result = s3.meta.client.generate_presigned_url('get_object', Params={'Bucket': results_bucket_name, 'Key': results_file_key}, ExpiresIn=60*2)
        except ClientError as e:
            error = e.response['Error']
            return error_response(500, f"Unable to generate presigned URL (result file): {error['Message']}")
        annotation['result_file_url'] = presigned_url_result

    return render_template('annotation_details.html', annotation=annotation, free_access_expired=free_access_expired)


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
    user_id = session.get('primary_identity')

    dynamo = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'], config=Config(signature_version = 's3v4'))

    # Exceptions found in dynamoDB boto documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
    try:
        dynamo_table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    except ClientError as e:
        error = e.response['Error']
        return error_response(500, f"Unable to access DynamoDB table: {error['Message']}")

    # Use of query() from boto documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
    try:
        response = dynamo_table.query(KeyConditionExpression=Key('job_id').eq(id))
    except ClientError as e:
        error = e.response['Error']
        return error_response(500, f"Unable to query DynamoDB table: {error['Message']}")

    annotation = response['Items'][0]
    if annotation['user_id'] != user_id:
       return error_response(400, 'Not authorized to view this job')

    bucket_name = annotation['s3_results_bucket']
    key = annotation['s3_key_log_file']

    s3 = boto3.resource('s3', region_name=app.config['AWS_REGION_NAME'], config=Config(signature_version = 's3v4'))

    # Accessing bucket and existence check from boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/migrations3.html
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        error = e.response['Error']
        if error['Code'] == '404':
            return error_response(500, f"{bucket_name} does not exist: {error['Message']}")

    # Get file from S3 bucket
    try:
        # get_object() from boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
        log = s3.meta.client.get_object(Bucket=bucket_name, Key=key)
    except ClientError as e:
        error = e.response['Error']
        return error_response(500, f"Unable to get object from s3 bucket: {error['Message']}")

    log_file_contents = str(log['Body'].read(), 'utf-8')
    return render_template('view_log.html', job_id=id, log_file_contents=log_file_contents)


"""Subscription management handler
"""
import stripe

@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
    if (request.method == 'GET'):
        # Display form to get subscriber credit card info
        if (session.get('role') == "free_user"):
            return render_template('subscribe.html')
        else:
            return redirect(url_for('profile'))

    elif (request.method == 'POST'):
        # Process the subscription request
        token = str(request.form['stripe_token']).strip()

    # Create a customer on Stripe
    stripe.api_key = app.config['STRIPE_SECRET_KEY']
    try:
        customer = stripe.Customer.create(
            card = token,
            plan = "premium_plan",
            email = session.get('email'),
            description = session.get('name')
        )
    except Exception as e:
        app.logger.error(f"Failed to create customer billing record: {e}")
        return abort(500)

    # Update user role to allow access to paid features
    update_profile(
        identity_id=session['primary_identity'],
        role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    # Add code here to initiate restoration of archived user data
    # Make sure you handle files not yet archived!

    # 1. Send message to restore queue with user_id
    # 2. Restore.py will pick up user_id and query all of their jobs
    # 3. In restore.py, loop through each job. If archived, then request restoration. If not archived, can skip because the object won't get archived by archive.py (it will just delete the message). Then as these are completed send a message to a queue with archive ID, job ID that thaw will poll.
    # 4. Thaw.py will long poll a SNS. Go through each message. Move restored archive to bucket, updated Dynamo.
    # Send message to restore topic
    data = {'user_id': session['primary_identity']}

    # Publish a notification message to the SNS topic
    sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'], config=Config(signature_version = 's3v4'))

    # Exceptions found in SNS publish() documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
    try:
        sns_publish = sns.publish(TopicArn=app.config['AWS_SNS_RESTORE_TOPIC'], Message=json.dumps(data), MessageStructure='string')
    except ClientError as e:
        error = e.response['Error']
        return error_response(500, f"Unable to publish message to restore SNS: {error['Message']}")

    # Display confirmation page
    return render_template('subscribe_confirm.html', 
        stripe_customer_id=str(customer['id']))


"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(
        identity_id=session['primary_identity'],
        role="free_user"
    )
    return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
    return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if (request.args.get('next')):
        session['next'] = request.args.get('next')
    return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
    return render_template('error.html', 
        title='Page not found', alert_level='warning',
        message="The page you tried to reach does not exist. \
          Please check the URL and try again."
        ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
    return render_template('error.html',
        title='Not authorized', alert_level='danger',
        message="You are not authorized to access this page. \
          If you think you deserve to be granted access, please contact the \
          supreme leader of the mutating genome revolutionary party."
        ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
    return render_template('error.html',
        title='Not allowed', alert_level='warning',
        message="You attempted an operation that's not allowed; \
          get your act together, hacker!"
        ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html',
        title='Server error', alert_level='danger',
        message="The server encountered an error and could \
          not process your request."
        ), 500

### EOF