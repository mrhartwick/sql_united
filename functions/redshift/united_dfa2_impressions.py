import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, when, lit
import time

## unzip the bundle with all the required modules
import zipfile

zip_ref = zipfile.ZipFile('pythonmodules_newest.zip', 'r')
zip_ref.extractall('./tmp/packages')
zip_ref.close()
sys.path.insert(0, './tmp/packages')

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


## This function is used to delete the files from S3 bucket.
def deleteProcessedFiles(args):
    s3path = args["s3Path"]
    pathWithoutS3 = s3path[5:]
    sepindex = pathWithoutS3.index("/")
    source_bucket_name = pathWithoutS3[:sepindex]
    prefix = pathWithoutS3[sepindex + 1:]
    source_prefix = prefix.replace('*', '')
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(source_bucket_name)
    result = my_bucket.objects.filter(Prefix=source_prefix)
    for o in result:
        if not o.key.endswith('/'):
            print 'File to archive: ' + o.key
            key = o.key
            s3.Object(source_bucket_name, key).delete()


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


args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME','s3Path','sender_address','recipient_address'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## Name of the table with schema name
tableName = "united.dfa2_impression"
databaseName = "wmprodfeeds"
s3Path = args["s3Path"]

try:
	df1 = spark.read.format("com.databricks.spark.csv").option("header", "false").option("inferSchema", "false").option("delimiter", '\t').load(s3Path)
except:
	sendNotification(args, 'Fail', 0, '', 'Error occurred while reading data')
	raise
## Add new column (AcquiredTime) to dataset. This column value is used to identify latest record in case of duplicate records.
currrentTime = time.strftime("%Y-%m-%d %H:%M")
df = df1.withColumn("AcquiredTime", lit(currrentTime))

datasource0 = DynamicFrame.fromDF(df, glueContext, "datasource0")
## Update datatypes
try:
	datasource1 = datasource0.resolveChoice(
		specs=[('_c0', 'cast:long'), ('_c1', 'cast:string'), ('_c2', 'cast:int'), ('_c3', 'cast:int'),
			   ('_c4', 'cast:int'), ('_c5', 'cast:int'),
			   ('_c6', 'cast:int'), ('_c7', 'cast:int'), ('_c8', 'cast:int'),
			   ('_c9', 'cast:string'), ('_c10', 'cast:string'),
			   ('_c11', 'cast:int'), ('_c12', 'cast:double'), ('_c13', 'cast:int'), ('_c14', 'cast:int'),
			   ('_c15', 'cast:int'), ('_c16', 'cast:string'), ('_c17', 'cast:string'), ('_c18', 'cast:string'),
			   ('_c19', 'cast:string'), ('_c20', 'cast:int'), ('_c21', 'cast:long'), ('_c22', 'cast:int'),
			   ('_c23', 'cast:int'), ('_c24', 'cast:int'), ('_c25', 'cast:int'), ('_c26', 'cast:int'),
			   ('_c27', 'cast:int'),
			   ('_c28', 'cast:int'), ('_c29', 'cast:string'), ('_c30', 'cast:long'), ('_c31', 'cast:string'),
			   ('_c32', 'cast:string'), ('_c33', 'cast:string'), ('_c34', 'cast:int'),
			   ('_c35', 'cast:string'), ('_c36', 'cast:boolean'), ('_c37', 'cast:int'),
			   ('_c38', 'cast:string'), ('_c39', 'cast:int'), ('_c40', 'cast:string'),
			   ('_c41', 'cast:int'), ('_c42', 'cast:int'), ('_c43', 'cast:int'),
			   ('_c44', 'cast:int'), ('_c45', 'cast:int'), ('_c46', 'cast:int'),
			   ('_c47', 'cast:string'), ('_c48', 'cast:int'), ('_c49', 'cast:int'),
			   ('_c50', 'cast:int'), ('_c51', 'cast:int'), ('_c52', 'cast:string'),
			   ('_c53', 'cast:int'), ('_c54', 'cast:int'), ('_c55', 'cast:int'),
			   ('_c56', 'cast:int'), ('_c57', 'cast:int'), ('_c58', 'cast:int'),
			   ('_c59', 'cast:int'), ('_c60', 'cast:int'), ('_c61', 'cast:int'),
			   ('_c62', 'cast:int'), ('_c63', 'cast:int'), ('_c64', 'cast:int'),
			   ('_c65', 'cast:int'), ('_c66', 'cast:int'), ('_c67', 'cast:int'),
			   ('_c68', 'cast:int'), ('_c69', 'cast:int'), ('_c70', 'cast:int'),
			   ('_c71', 'cast:int'), ('_c72', 'cast:int'), ('_c73', 'cast:int'),
			   ('_c74', 'cast:int'), ('_c75', 'cast:int'), ('_c76', 'cast:int'),
			   ('_c77', 'cast:int'), ('_c78', 'cast:int'), ('_c79', 'cast:int'),
			   ('_c80', 'cast:int'), ('_c81', 'cast:int'), ('_c82', 'cast:int'),
			   ('_c83', 'cast:int'), ('_c84', 'cast:int'), ('_c85', 'cast:int'),
			   ('_c86', 'cast:int'), ('_c87', 'cast:int'), ('_c88', 'cast:int'),
			   ('_c89', 'cast:int'), ('_c90', 'cast:int'), ('_c91', 'cast:int'),
			   ('_c92', 'cast:int'), ('_c93', 'cast:int'), ('_c94', 'cast:int'),
			   ('_c95', 'cast:int'), ('_c96', 'cast:int'), ('_c97', 'cast:int'),
			   ('_c98', 'cast:int'), ('_c99', 'cast:int'), ('_c100', 'cast:int'),
			   ('_c101', 'cast:long'), ('_c102', 'cast:boolean'), ('_c103', 'cast:timestamp'),
			   ('_c104', 'cast:timestamp'),('_c105', 'cast:int'),
			   ('AcquiredTime', 'cast:timestamp')])
except:
    sendNotification(args, 'Fail', 0, '', 'Error occurred while updating datatypes')
    raise
## Mapping of source column and target column including data types.
applymapping1 = ApplyMapping.apply(frame=datasource1, mappings=[("_c0", "long", "event_time", "long"),
																("_c1", "string", "user_id", "string"),
																("_c2", "int", "advertiser_id", "int"),
																("_c3", "int", "campaign_id", "int"),
																("_c4", "int", "ad_id", "int"),
																("_c5", "int", "rendering_id", "int"),
																("_c6", "int", "creative_version", "int"),
																("_c7", "int", "site_id_dcm", "int"),
																("_c8", "int", "placement_id", "int"),
																("_c9", "string", "country_code", "string"),
																("_c10", "string", "state_region", "string"),
																("_c11", "int", "browser_platform_id", "int"),
																("_c12", "double", "browser_platform_version", "double"),
																("_c13", "int", "operating_system_id", "int"),
																("_c14", "int", "designated_market_area_dma_id", "int"),
																("_c15", "int", "city_id", "int"),
																("_c16", "string", "zip_postal_code", "string"),
																("_c17", "string", "u_value", "string"),
																("_c18", "string", "event_type", "string"),
																("_c19", "string", "event_sub_type", "string"),
																("_c20", "string", "dbm_auction_id", "string"),
																("_c21", "long", "dbm_request_time", "long"),
																("_c22", "int", "dbm_advertiser_id", "int"),
																("_c23", "int", "dbm_insertion_order_id", "int"),
																("_c24", "int", "dbm_line_item_id", "int"),
																("_c25", "int", "dbm_creative_id", "int"),
																("_c26", "int", "dbm_bid_price_usd", "int"),
																("_c27", "int", "dbm_bid_price_partner_currency", "int"),
																("_c28", "int", "dbm_bid_price_advertiser_currency","int"),
																("_c29", "string", "dbm_url", "string"),
																("_c30", "long", "dbm_site_id", "long"),
																("_c31", "string", "dbm_language", "string"),
																("_c32", "string", "dbm_adx_page_categories", "string"),
																("_c33", "string", "dbm_matching_targeted_keywords","string"),
																("_c34", "int", "dbm_exchange_id", "int"),
																("_c35", "string","dbm_attributed_inventory_source_external_id","string"),
																("_c36", "boolean","dbm_attributed_inventory_source_is_public","boolean"),
																("_c37", "int", "dbm_ad_position", "int"),
																("_c38", "string", "dbm_country_code", "string"),
																("_c39", "int", "dbm_designated_market_area_dma_id","int"),
																("_c40", "string", "dbm_zip_postal_code", "string"),
																("_c41", "int", "dbm_state_region_id", "int"),
																("_c42", "int", "dbm_city_id", "int"),
																("_c43", "int", "dbm_operating_system_id", "int"),
																("_c44", "int", "dbm_browser_platform_id", "int"),
																("_c45", "int", "dbm_browser_timezone_offset_minutes","int"),
																("_c46", "int", "dbm_net_speed", "int"),
																("_c47", "string", "dbm_matching_targeted_segments","string"),
																("_c48", "int", "dbm_isp_id", "int"),
																("_c49", "int", "dbm_device_type", "int"),
																("_c50", "int", "dbm_mobile_make_id", "int"),
																("_c51", "int", "dbm_mobile_model_id", "int"),
																("_c52", "string", "dbm_view_state", "string"),
																("_c53", "int", "dbm_media_cost_usd", "int"),
																("_c54", "int", "dbm_media_cost_partner_currency","int"),
																("_c55", "int", "dbm_media_cost_advertiser_currency","int"),
																("_c56", "int", "dbm_revenue_usd", "int"),
																("_c57", "int", "dbm_revenue_partner_currency", "int"),
																("_c58", "int", "dbm_revenue_advertiser_currency","int"),
																("_c59", "int", "dbm_total_media_cost_usd", "int"),
																("_c60", "int", "dbm_total_media_cost_partner_currency","int"),
																("_c61", "int","dbm_total_media_cost_advertiser_currency", "int"),
																("_c62", "int", "dbm_cpm_fee_1_usd", "int"),
																("_c63", "int", "dbm_cpm_fee_1_partner_currency", "int"),
																("_c64", "int", "dbm_cpm_fee_1_advertiser_currency","int"),
																("_c65", "int", "dbm_cpm_fee_2_usd", "int"),
																("_c66", "int", "dbm_cpm_fee_2_partner_currency", "int"),
																("_c67", "int", "dbm_cpm_fee_2_advertiser_currency","int"),
																("_c68", "int", "dbm_cpm_fee_3_usd", "int"),
																("_c69", "int", "dbm_cpm_fee_3_partner_currency", "int"),
																("_c70", "int", "dbm_cpm_fee_3_advertiser_currency","int"),
																("_c71", "int", "dbm_cpm_fee_4_usd", "int"), (
																"_c72", "int", "dbm_cpm_fee_4_partner_currency", "int"),
																("_c73", "int", "dbm_cpm_fee_4_advertiser_currency",
																 "int"), ("_c74", "int", "dbm_cpm_fee_5_usd", "int"),
																("_c75", "int", "dbm_cpm_fee_5_partner_currency", "int"),
																("_c76", "int", "dbm_cpm_fee_5_advertiser_currency",
																 "int"), ("_c77", "int", "dbm_media_fee_1_usd", "int"),
																("_c78", "int", "dbm_media_fee_1_partner_currency","int"),
																("_c79", "int", "dbm_media_fee_1_advertiser_currency",
																 "int"), ("_c80", "int", "dbm_media_fee_2_usd", "int"),
																("_c81", "int", "dbm_media_fee_2_partner_currency","int"),
																("_c82", "int", "dbm_media_fee_2_advertiser_currency","int"),
																("_c83", "int", "dbm_media_fee_3_usd", "int"),
																("_c84", "int", "dbm_media_fee_3_partner_currency","int"), (
																"_c85", "int", "dbm_media_fee_3_advertiser_currency","int"),
																("_c86", "int", "dbm_media_fee_4_usd", "int"),
																("_c87", "int", "dbm_media_fee_4_partner_currency","int"),
																("_c88", "int", "dbm_media_fee_4_advertiser_currency","int"),
																("_c89", "int", "dbm_media_fee_5_usd", "int"),
																("_c90", "int", "dbm_media_fee_5_partner_currency","int"),
																("_c91", "int", "dbm_media_fee_5_advertiser_currency","int"),
																("_c92", "int", "dbm_data_fee_usd", "int"),
																("_c93", "int", "dbm_data_fee_partner_currency", "int"),
																("_c94", "int", "dbm_data_fee_advertiser_currency","int"),
																("_c95", "int", "dbm_billable_cost_usd", "int"),
																("_c96", "int", "dbm_billable_cost_partner_currency","int"),
																("_c97", "int", "dbm_billable_cost_advertiser_currency","int"),
																("_c98", "int", "active_view_eligible_impressions","int"),
																("_c99", "int", "active_view_measurable_impressions","int"),
																("_c100", "int", "active_view_viewable_impressions","int"),
																("_c101", "long", "md_user_id_numeric", "long"),
																("_c102", "boolean", "md_user_id_0", "boolean"),
																("_c103", "timestamp", "md_event_time", "timestamp"),
																("_c104", "timestamp", "md_dbm_request_time", "timestamp"),
																("_c105", "int", "md_file_date", "int"),
																("AcquiredTime", "timestamp", "AcquiredTime",
																"timestamp")], transformation_ctx="applymapping1")

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
print 'Delete Processed files'
try:
    print 'Delete Processed files'
    deleteProcessedFiles(args)
except:
	print 'Error while deleting files'
sendNotification(args, 'Successful', recordCount, tableName, '')