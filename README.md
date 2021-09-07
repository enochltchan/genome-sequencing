# Genome Sequencing Web App

Directory contents are as follows:
* `/web` - The genome sequencing web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

# Functionality
The app allows a user to perform the following functions:

* Log in (via Globus Auth) to use the service -- Some aspects of the service are available only to registered users. Two classes of users will be supported: Free and Premium. Premium users will have access to additional functionality, beyond that available to Free users.

* Upgrade from a Free to a Premium user -- Premium users will be required to provide a credit card for payment of the service subscription. The app will integrate with Stripe for credit card payment processing. No real credit cards are required for this project; we use a test credit card (4242424242424242) provided by Stripe.

* Submit an annotation job -- Free users may only submit jobs of up to a certain size. Premium users may submit any size job. If a Free user submits an oversized job, the system will refuse it and will prompt the user to convert to a Premium user.

* Receive notifications when annotation jobs finish -- When their annotation request is complete, the app will send users an email that includes a link where they can view the log file and download the results file.

* Browse jobs and download annotation results -- The app will store annotation results for later retrieval. Users may view a list of their jobs (completed and running). Free users may download results up to 10 minutes after their job has completed; thereafter their results will be archived and only available to them if they convert to a Premium user. Premium users will always have all their data available for download.
