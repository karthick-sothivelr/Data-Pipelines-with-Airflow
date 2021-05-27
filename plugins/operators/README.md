Project: Data Pipelines with Airflow
-------------------------------------
Summary:
	Sparkify (music streaming company) wants to introduce more automation and monitoring to their data warehouse ETL pipelines using Apache Airflow.
	
	The main goals of the project are as follows:
		1) Create high grade data pipelines that are dynamic and built from reusable tasks
		2) The tasks should allow monitoring and easy backfills
		3) Perform data quality checks after the ETL steps

Dataset Location:
	The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift
	Source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.
	Data Location:
		Song data: s3://udacity-dend/song_data
		Log data: s3://udacity-dend/log_data

Create custom operators to perform tasks such as:
	1) Staging the data
	2) Filling the data warehouse
	3) Running checks on the data