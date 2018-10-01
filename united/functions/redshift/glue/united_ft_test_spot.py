import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lit, udf, when
from pyspark.sql.types import *
import datetime
import time
from datetime import timedelta


## unzip the bundle with all the required modules
import zipfile

zip_ref = zipfile.ZipFile('pythonmodules_newest.zip', 'r')
zip_ref.extractall('./tmp/packages')
zip_ref.close()
sys.path.insert(0, './tmp/packages')

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import pytz
from pytz import timezone, tzinfo


## This function is used to delete the files from S3 bucket.
# def deleteProcessedFiles(args):
#     s3path = args["s3Path"]
#     pathWithoutS3 = s3path[5:]
#     sepindex = pathWithoutS3.index("/")
#     source_bucket_name = pathWithoutS3[:sepindex]
#     prefix = pathWithoutS3[sepindex + 1:]
#     source_prefix = prefix.replace('*', '')
#     s3 = boto3.resource('s3')
#     my_bucket = s3.Bucket(source_bucket_name)
#     result = my_bucket.objects.filter(Prefix=source_prefix)
#     for o in result:
#         if not o.key.endswith('/'):
#             print 'File to archive: ' + o.key
#             key = o.key
#             s3.Object(source_bucket_name, key).delete()

## This function is used to send email notification to users.
def sendNotification(args, status, noOfRecords, tableName, errorMsg):
	sender = args["sender_address"]
	toAddresses = args["recipient_address"]
	recipient = toAddresses.split(",")
	aws_region = "us-east-1"
	subject = "Glue job execution status: " + status
	# The email body for recipients with non-HTML email clients.
	if status == 'Successful':
		body_text = (args["JOB_NAME"] + " runs successfully. \r\n"
										"Loaded " + str(noOfRecords) + " records into " + tableName + " table."
					 )

		# The HTML body of the email.
		body_html = """<html>
		<head></head>
		<body>
		  <h1>""" + args["JOB_NAME"] + """ executed successfully. </h1>
		  <p>Loaded """ + str(noOfRecords) + """ records into """ + tableName + """ table.</p>
		</body>
		</html>"""
	else:
		body_text = (args["JOB_NAME"] + " failed. \r\n"
										"Error Message: " + errorMsg + "\r\n"
																	   "Please refer cloudwatch logs for details."
					 )

		# The HTML body of the email.
		body_html = """<html>
		<head></head>
		<body>
		  <h1>""" + args["JOB_NAME"] + """ failed. </h1>
		  <p>Error Message: """ + errorMsg + """</p>
		  <p>Please refer cloudwatch logs for details.</p>
		</body>
		</html>"""
		# The character encoding for the email.
	charset = "UTF-8"

	# Create a new SES resource and specify a region.
	client = boto3.client('ses', region_name=aws_region)
	print 'Sending email'
	# Try to send the email.
	try:
		# Provide the contents of the email.
		response = client.send_email(
			Destination={
				'ToAddresses':
					recipient
				,
			},
			Message={
				'Body': {
					'Html': {
						'Charset': charset,
						'Data': body_html,
					},
					'Text': {
						'Charset': charset,
						'Data': body_text,
					},
				},
				'Subject': {
					'Charset': charset,
					'Data': subject,
				},
			},
			Source=sender,
		)
	except ClientError as e:
		print('error' + e.response['Error']['Message'])
	else:
		print("Email sent! Message ID:")




dtm_fmt = "%Y%m%d-%H:%M:%S"
tim_fmt = "%Y-%m-%d %H:%M:%S.%f"
dat_fmt = "%Y-%m-%d"


# Convert Unix epoch to timestamp
def convert_to_ts(value):
   if not (value == '' or value is None):
        try:
           # dtm_fmt = "%Y%m%d-%H:%M:%S"
           result = datetime.datetime.strptime(value,"%Y%m%d-%H:%M:%S")
           return result
        except:
           return None

def convert_to_ts_2(value):
   if not (value == '' or value is None):
        try:
           # dtm_fmt = "%Y%m%d-%H:%M:%S"
           result = datetime.datetime.strptime(value,"%Y%m%d-%H:%M:%S")
           return result
        except:
           return None

args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME','s3Path','sender_address','recipient_address'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## Name of the table with schema name
tableName = "united.ft_test_spot"
databaseName = "wmprodfeeds"
s3Path = args["s3Path"]

try:
	df1 = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").option("delimiter", '\t').load(s3Path)
except:
	sendNotification(args, 'Fail', 0, '', 'Error occurred while reading data')
	raise

## Add new column (AcquiredTime) to dataset. This column value is used to identify latest record in case of duplicate records.

# currrent_time = datetime.datetime.now().strftime(tm_fmt)


udf_convert_to_ts 	= udf(convert_to_ts,   TimestampType())
udf_convert_to_ts_2 = udf(convert_to_ts_2, TimestampType())

try:
	df = df1.withColumn("event_time", udf_convert_to_ts(df1.Time).cast("timestamp"))
	# df = df1.withColumn("event_time", udf_convert_to_ts(df1.Time))

except:
	sendNotification(args, 'Fail', 0, '', 'Error occurred while formatting columns')
	raise

datasource0 = DynamicFrame.fromDF(df1, glueContext, "datasource0")
## Update datatypes
try:
	datasource1 = datasource0.resolveChoice(
		specs=[
			   ('Time', 'cast:string'),
			   ('User-ID', 'cast:string'),
			   ('Associated-Event-Time', 'cast:string'),
			   ('Associated-Match-ID', 'cast:int'),
			   ('IP', 'cast:string'),
			   ('Advertiser-ID', 'cast:int'),
			   ('Campaign-ID', 'cast:int'),
			   ('Site-ID', 'cast:int'),
			   ('Placement-ID', 'cast:int'),
			   ('Creative-ID', 'cast:int'),
			   ('Version-ID', 'cast:int'),
			   ('Spot-Group-ID', 'cast:int'),
			   ('Spot-ID', 'cast:int'),
			   ('Country-ID', 'cast:int'),
			   ('State-ID', 'cast:int'),
			   ('DMA-ID', 'cast:int'),
			   ('City-ID', 'cast:int'),
			   ('Browser-ID', 'cast:int'),
			   ('Device-ID', 'cast:int'),
			   ('OS-ID', 'cast:int'),
			   ('Connection-ID', 'cast:int'),
			   ('Transaction-Value', 'cast:string'),
			   ('Transaction-ID', 'cast:string'),
			   ('Query-String', 'cast:string'),
			   ('Associated-Event-Type', 'cast:int'),
			   ('Associated-Viewability', 'cast:string'),
			   ('Product-Code', 'cast:string'),
			   ('Custom-Data', 'cast:string'),
			   ('Device9-ID', 'cast:string'),
			   ('Impression-ID', 'cast:string'),
			   ('event_time', 'cast:timestamp'),
			   ('interaction_time', 'cast:timestamp')
			   ])

except:
    sendNotification(args, 'Fail', 0, '', 'Error occurred while updating datatypes')
    raise
## Mapping of source column and target column including data types.
applymapping1 = ApplyMapping.apply(frame=datasource1, mappings=[
																("Time", "string", "ft_time", "string"),
																("User-ID","string","user_id","string"),
																("Associated-Event-Time","string","associated_event_time","string"),
																("Associated-Match-ID","int","associated_match_id","int"),
																("IP","string","ip","string"),
																("Advertiser-ID","int","advertiser_id","int"),
																("Campaign-ID","int","campaign_id","int"),
																("Site-ID","int","site_id","int"),
																("Placement-ID","int","placement_id","int"),
																("Creative-ID","int","creative_id","int"),
																("Version-ID","int","version_id","int"),
																("Spot-Group-ID","int","spot_group_id","int"),
																("Spot-ID","int","spot_id","int"),
																("Country-ID","int","country_id","int"),
																("State-ID","int","state_id","int"),
																("DMA-ID","int","dma_id","int"),
																("City-ID","int","city_id","int"),
																("Browser-ID","int","browser_id","int"),
																("Device-ID","int","device_id","int"),
																("OS-ID","int","os_id","int"),
																("Connection-ID","int","connection_id","int"),
																("Transaction-Value","string","transaction_value","string"),
																("Transaction-ID","string","transaction_id","string"),
																("Query-String","string","query_string","string"),
																("Associated-Event-Type","int","associated_event_type","int"),
																("Associated-Viewability","string","associated_viewability","string"),
																("Product-Code","string","product_code","string"),
																("Custom-Data","string","custom_data","string"),
																("Device9-ID","string","device9_id","string"),
																("Impression-ID","string","impression_id","string"),
																("event_time","timestamp","event_time","timestamp"),
																("interaction_time","timestamp","interaction_time","timestamp")
																], transformation_ctx="applymapping1")

## Drops all null fields in this DynamicFrame.
dropnullfields3 = DropNullFields.apply(frame=applymapping1, transformation_ctx="dropnullfields3")
recordCount = dropnullfields3.count()

## Load data into Redshift
try:
	datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
															   catalog_connection="NewRedshiftConnection",
															   connection_options=
															   {
																   "dbtable": tableName,
																   "database": databaseName
															   }, redshift_tmp_dir=args["TempDir"],
															   transformation_ctx="datasink4")
	job.commit()
except:
	sendNotification(args, 'Fail', 0, '', 'Error occurred while loading data into table')
	raise
# try:
#     print 'Delete Processed files'
#     deleteProcessedFiles(args)
# except:
# 	print 'Error while deleting files'
sendNotification(args, 'Successful', recordCount, tableName, '')