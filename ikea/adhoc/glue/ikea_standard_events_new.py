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

zip_ref = zipfile.ZipFile('pythonmodules_glue.zip', 'r')
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
def sendNotification(args,status,noOfRecords, tableName, errorMsg):
    sender = args["sender_address"]
    toAddresses = args["recipient_address"]
    recipient = toAddresses.split(",")
    aws_region = "us-east-1"
    subject = "Glue job execution status: "+ status
    # The email body for recipients with non-HTML email clients.
    if status == 'Successful':
        body_text = (args["JOB_NAME"] + " runs successfully. \r\n"
                     "Loaded "+ str(noOfRecords) + " records into " + tableName + " table."
                    )

        # The HTML body of the email.
        body_html = """<html>
        <head></head>
        <body>
          <h1>""" + args["JOB_NAME"] + """ executed successfully. </h1>
          <p>Loaded """+ str(noOfRecords) + """ records into """ + tableName +""" table.</p>
        </body>
        </html>"""
    else:
        body_text = (args["JOB_NAME"] + " failed. \r\n"
                     "Error Message: "+ errorMsg  + "\r\n"
                     "Please refer cloudwatch logs for details."
                    )

        # The HTML body of the email.
        body_html = """<html>
        <head></head>
        <body>
          <h1>""" + args["JOB_NAME"] + """ failed. </h1>
          <p>Error Message: """+  errorMsg  + """</p>
          <p>Please refer cloudwatch logs for details.</p>
        </body>
        </html>"""
    # The character encoding for the email.
    charset = "UTF-8"

    # Create a new SES resource and specify a region.
    client = boto3.client('ses',region_name=aws_region)
    print 'Sending email'
    # Try to send the email.
    try:
        #Provide the contents of the email.
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
sqlContext = SQLContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## Name of the table with schema name
tableName = "ikea.sizmek_standard_events"
s3Path = args["s3Path"]

## This function is used to replace empty string by NULL /None.
def replaceby_null(x):
    return when(col(x) != "\N", col(x)).otherwise(None)

try:
    df1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("inferSchema", "true").option("delimiter", '').load(s3Path)
except:
    sendNotification(args, 'Fail',0,'','Error occurred while reading data')
    raise
df1 = df1.withColumn("_c29", replaceby_null("_c29"))
## Add new column (AcquiredTime) to dataset. This column value is used to identify latest record in case of duplicate records.
currrentTime = time.strftime("%Y-%m-%d %H:%M")
df = df1.withColumn("AcquiredTime", lit(currrentTime))

datasource0 = DynamicFrame.fromDF(df, glueContext, "datasource0")
## Update datatypes
try:
    datasource1 = datasource0.resolveChoice(
        specs=[
('_c0','cast:string'),
('_c1','cast:string'),
('_c2','cast:int'),
('_c3','cast:timestamp'),
('_c4','cast:int'),
('_c5','cast:int'),
('_c6','cast:string'),
('_c7','cast:int'),
('_c8','cast:int'),
('_c9','cast:int'),
('_c10','cast:int'),
('_c11','cast:int'),
('_c12','cast:int'),
('_c13','cast:long'),
('_c14','cast:int'),
('_c15','cast:int'),
('_c16','cast:int'),
('_c17','cast:int'),
('_c18','cast:string'),
('_c19','cast:string'),
('_c20','cast:int'),
('_c21','cast:int'),
('_c22','cast:string'),
('_c23','cast:string'),
('_c24','cast:string'),
('_c25','cast:int'),
('_c26','cast:int'),
('_c27','cast:int'),
('_c28','cast:string'),
('_c29','cast:int'),
('_c30','cast:long'),
('_c31','cast:string'),
('_c32','cast:long'),
('_c33','cast:string'),
('_c34','cast:int'),
('_c35','cast:int'),
('_c36','cast:int'),
('_c37','cast:int'),
('_c38','cast:int'),
('_c39','cast:int'),
('_c40','cast:string'),
('_c41','cast:int'),
('_c42','cast:string'),
('_c43','cast:string'),
('_c44','cast:timestamp'),
('_c45','cast:long'),
('_c46','cast:int'),
('_c47','cast:long'),
('AcquiredTime','cast:timestamp')


           ])
except:
    sendNotification(args, 'Fail',0,'','Error occurred while updating datatypes')
    raise
## Mapping of source column and target column including data types.
applymapping1 = ApplyMapping.apply(frame=datasource1, mappings=
[
("_c0",  "string", "EventID", "string"),
("_c1",  "string", "UserID", "string"),
("_c2",  "int", "EventTypeID", "int"),
("_c3",  "timestamp", "EventDate", "timestamp"),
("_c4",  "int", "EntityID", "int"),
("_c5",  "int", "SEMID", "int"),
("_c6",  "string", "SEUniqueID", "string"),
("_c7",  "int", "PlacementID", "int"),
("_c8",  "int", "SiteID", "int"),
("_c9",  "int", "CampaignID", "int"),
("_c10", "int", "BrandID", "int"),
("_c11", "int", "AdvertiserID", "int"),
("_c12", "int", "AccountID", "int"),
("_c13", "long", "SearchAdID", "long"),
("_c14", "int", "AdGroupID", "int"),
("_c15", "int", "CountryID", "int"),
("_c16", "int", "StateID", "int"),
("_c17", "int", "DMAID", "int"),
("_c18", "string", "ZipCode", "string"),
("_c19", "string", "AreaCode", "string"),
("_c20", "int", "BrowserCode", "int"),
("_c21", "int", "OSCode", "int"),
("_c22", "string", "Referrer", "string"),
("_c23", "string", "MobileDevice", "string"),
("_c24", "string", "MobileCarrier", "string"),
("_c25", "int", "AudienceID", "int"),
("_c26", "int", "ProductID", "int"),
("_c27", "int", "CityID", "int"),
("_c28", "string", "PCP", "string"),
("_c29", "int", "EBCampaignID", "int"),
("_c30", "long", "SECampaignID", "long"),
("_c31", "string", "SEAccountID", "string"),
("_c32", "long", "SEAdGroupID", "long"),
("_c33", "string", "versions", "string"),
("_c34", "int", "deliveryGroupID", "int"),
("_c35", "int", "Price", "int"),
("_c36", "int", "ExchangeId", "int"),
("_c37", "int", "BidID", "int"),
("_c38", "int", "StrategyID", "int"),
("_c39", "int", "ImpressionTypeID", "int"),
("_c40", "string", "ImpressionExchangeToken", "string"),
("_c41", "int", "BuyID", "int"),
("_c42", "string", "InventorySource", "string"),
("_c43", "string", "DSP_ID", "string"),
("_c44", "timestamp", "EventDateDefaultTImeZone", "timestamp"),
("_c45", "long", "UserIDNumeric", "long"),
("_c46", "int", "md_file_date", "int"),
("_c47", "long", "md_user_id_numeric", "long"),
("AcquiredTime", "timestamp", "AcquiredTime", "timestamp")
 ], transformation_ctx="applymapping1")

## Drops all null fields in this DynamicFrame.
dropnullfields3 = DropNullFields.apply(frame=applymapping1, transformation_ctx="dropnullfields3")
recordCount = dropnullfields3.count()

## Load data into Redshift
try:
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3, catalog_connection="NewRedshiftConnection", connection_options =
        {
            "dbtable": tableName,
            "database": "wmprodfeeds"
        }, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")
    job.commit()
except:
    sendNotification(args, 'Fail',0,'','Error occurred while loading data into table')
    raise
# print 'Delete Processed files'
# try:
#     deleteProcessedFiles(args)
# except:
#     print 'Error while deleting files'
sendNotification(args, 'Successful',recordCount,tableName,'')