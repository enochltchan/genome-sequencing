# gas-framework
An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

# Q7: Data Archival Implementation Notes
Free users can only download their data for up to 5 minutes after completion of their annotation job. After that, their results data will be archived to a Glacier vault. I implement this using a message queue. Specifically, I create an SNS topic that is linked to an SQS queue called enochltchan_job_archives. When run.py finishes processing an annotation job, it immediately sends a message to enochltchan_job_archives. The message includes two pieces of data: job_id and user_id. On the utility instance, archive.py reads messages off of this queue. However, it sets the DelaySeconds attribute of the queue to be 5 minutes, meaning that any message that is sent to the queue will not be able to be read by archive.py until 5 minutes afterwards. This corresponds precisely to the 5 minutes of time that a Free user has to download their data until it becomes archived. Once the 5 minutes is up, archive.py will read the message, then initiate the archival process using the job_id and user_id, moving the user's data to Glacier. archive.py will then persist the id of the Glacier archive in DynamoDB, while also removing the s3_key_result_file from DynamoDB. This is done to ensure that the database accurately reflects information in both Glacier and S3 - the user's data is no longer in S3, so the key is removed. On the front end, views.py also will no longer show the "download file" link to the user after 5 minutes, but will instead prompt the user to upgrade to Premium. This implementation was chosen because it allows the annotation process and data archival process to be decoupled on separate instances that can be individually scaled if needed. The queue system is crucial in achieving this decoupling.

# Q9: Data Restoration Implementation Notes
When a Free user upgrades to a Premium user, that user’s results files are moved from the Glacier vault back to the gas-results S3 bucket. I implement this using message queues. Specifically, I create an SNS topic that is linked to an SQS queue called enochltchan_restore. When a user upgrades to premium via the subscribe() function in views.py, the code will immediately send a message to enochltchan_restore. The message includes user_id data. On the utility instance, restore.py reads messages off of this queue. It takes user_id data and pulls all the user's jobs from DynamoDB. For jobs that have been archived, it begins the restoration process, attempting the expedited restoration and degrading to the standard restoration. When it initiates a restoration, it specifies a second SNS topic for Glacier to send a notification to when the job is complete. The user_id is sent as part of the notification (specifically, in the JobDescription), as well as the job_id and archive_id. The second SNS topic is linked to a second SQS queue called enochltchan_thaw. On the utility instance, thaw.py reads messages off of this queue. It takes the user_id, job_id and archive_id from the message to locate the restored file, upload the file to S3, then update the corresponding DynamoDB entry to remove the id of the Glacier archive and add back the s3_key_result_file. This two-queue implementation was chosen because it allows the upgrade process, restoration process, and upload process to be decoupled on separate instances that can be individually scaled if needed. The queue system is crucial in achieving this decoupling. In addition, this implementation takes advantage of Glacier's built-in functionality to send notifications to an SNS topic once restoration jobs are complete, rather than having to rely on long-polling Glacier.