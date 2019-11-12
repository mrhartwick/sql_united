import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, when,lit
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
tableName = "ikea.sizmek_conversion_events"
s3Path = args["s3Path"]

## This function is used to replace empty string by NULL /None.
def replaceby_null(x):
    return when(col(x) != "", col(x)).otherwise(None)

## Read data from S3 location
try:
    df1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("inferSchema", "true").option("delimiter", '').load(s3Path)
except:
    sendNotification(args, 'Fail',0,'','Error occurred while reading data')
    raise

## Add new column (AcquiredTime) to dataset. This column value is used to identify latest record in case of duplicate records.
currrentTime = time.strftime("%Y-%m-%d %H:%M")
df = df1.withColumn("AcquiredTime", lit(currrentTime))

datasource0 = DynamicFrame.fromDF(df, glueContext, "datasource0")
## Update datatypes
try:
    datasource1 = datasource0.resolveChoice(specs = [
('_c0','cast:string'),
('_c1','cast:string'),
('_c2','cast:timestamp'),
('_c3','cast:int'),
('_c4','cast:int'),
('_c5','cast:int'),
('_c6','cast:double'),
('_c7','cast:int'),
('_c8','cast:int'),
('_c9','cast:string'),
('_c10','cast:string'),
('_c11','cast:string'),
('_c12','cast:string'),
('_c13','cast:int'),
('_c14','cast:int'),
('_c15','cast:string'),
('_c16','cast:int'),
('_c17','cast:timestamp'),
('_c18','cast:int'),
('_c19','cast:int'),
('_c20','cast:int'),
('_c21','cast:int'),
('_c22','cast:int'),
('_c23','cast:int'),
('_c24','cast:int'),
('_c25','cast:int'),
('_c26','cast:int'),
('_c27','cast:string'),
('_c28','cast:string'),
('_c29','cast:int'),
('_c30','cast:int'),
('_c31','cast:string'),
('_c32','cast:string'),
('_c33','cast:string'),
('_c34','cast:string'),
('_c35','cast:string'),
('_c36','cast:string'),
('_c37','cast:string'),
('_c38','cast:string'),
('_c39','cast:string'),
('_c40','cast:string'),
('_c41','cast:string'),
('_c42','cast:string'),
('_c43','cast:string'),
('_c44','cast:string'),
('_c45','cast:string'),
('_c46','cast:string'),
('_c47','cast:string'),
('_c48','cast:string'),
('_c49','cast:string'),
('_c50','cast:string'),
('_c51','cast:string'),
('_c52','cast:string'),
('_c53','cast:string'),
('_c54','cast:string'),
('_c55','cast:string'),
('_c56','cast:string'),
('_c57','cast:string'),
('_c58','cast:string'),
('_c59','cast:string'),
('_c60','cast:string'),
('_c61','cast:string'),
('_c62','cast:string'),
('_c63','cast:string'),
('_c64','cast:string'),
('_c65','cast:string'),
('_c66','cast:string'),
('_c67','cast:string'),
('_c68','cast:string'),
('_c69','cast:string'),
('_c70','cast:string'),
('_c71','cast:string'),
('_c72','cast:string'),
('_c73','cast:string'),
('_c74','cast:string'),
('_c75','cast:string'),
('_c76','cast:string'),
('_c77','cast:string'),
('_c78','cast:string'),
('_c79','cast:string'),
('_c80','cast:string'),
('_c81','cast:int'),
('_c82','cast:long'),
('_c83','cast:string'),
('_c84','cast:long'),
('_c85','cast:int'),
('_c86','cast:int'),
('_c87','cast:int'),
('_c88','cast:int'),
('_c89','cast:boolean'),
('_c90','cast:string'),
('_c91','cast:string'),
('_c92','cast:string'),
('_c93','cast:timestamp'),
('_c94','cast:timestamp'),
('_c95','cast:long'),
('_c96','cast:int'),
('_c97','cast:long'),
('AcquiredTime','cast:timestamp')])
except:
    sendNotification(args, 'Fail',0,'','Error occurred while updating datatypes')
    raise

## Mapping of source column and target column including data types.
applymapping1 = ApplyMapping.apply(frame = datasource1, mappings =
[
("_c0", "string", "UserID", "string"),
("_c1", "string", "ConversionID", "string"),
("_c2", "timestamp", "ConversionDate", "timestamp"),
("_c3", "int", "ConversionTagID", "int"),
("_c4", "int", "AdvertiserID", "int"),
("_c5", "int", "AccountID", "int"),
("_c6", "double", "Revenue", "double"),
("_c7", "int", "Currency", "int"),
("_c8", "int", "Quantity", "int"),
("_c9", "string", "OrderID", "string"),
("_c10", "string", "Referrer", "string"),
("_c11", "string", "ProductID", "string"),
("_c12", "string", "ProductInfo", "string"),
("_c13", "int", "WinnerEntityID", "int"),
("_c14", "int", "WinnerSEMID", "int"),
("_c15", "string", "WinnerSEUniqueID", "string"),
("_c16", "int", "EventTypeID", "int"),
("_c17", "timestamp", "WinnerEventDate", "timestamp"),
("_c18", "int", "PlacementID", "int"),
("_c19", "int", "SiteID", "int"),
("_c20", "int", "CampaignID", "int"),
("_c21", "int", "AdGroupID", "int"),
("_c22", "int", "BrandID", "int"),
("_c23", "int", "CountryID", "int"),
("_c24", "int", "StateID", "int"),
("_c25", "int", "DMAID", "int"),
("_c26", "int", "CityID", "int"),
("_c27", "string", "ZipCode", "string"),
("_c28", "string", "AreaCode", "string"),
("_c29", "int", "OSCode", "int"),
("_c30", "int", "BrowserCode", "int"),
("_c31", "string", "String1", "string"),
("_c32", "string", "String2", "string"),
("_c33", "string", "String3", "string"),
("_c34", "string", "String4", "string"),
("_c35", "string", "String5", "string"),
("_c36", "string", "String6", "string"),
("_c37", "string", "String7", "string"),
("_c38", "string", "String8", "string"),
("_c39", "string", "String9", "string"),
("_c40", "string", "String10", "string"),
("_c41", "string", "String11", "string"),
("_c42", "string", "String12", "string"),
("_c43", "string", "String13", "string"),
("_c44", "string", "String14", "string"),
("_c45", "string", "String15", "string"),
("_c46", "string", "String16", "string"),
("_c47", "string", "String17", "string"),
("_c48", "string", "String18", "string"),
("_c49", "string", "String19", "string"),
("_c50", "string", "String20", "string"),
("_c51", "string", "String21", "string"),
("_c52", "string", "String22", "string"),
("_c53", "string", "String23", "string"),
("_c54", "string", "String24", "string"),
("_c55", "string", "String25", "string"),
("_c56", "string", "String26", "string"),
("_c57", "string", "String27", "string"),
("_c58", "string", "String28", "string"),
("_c59", "string", "String29", "string"),
("_c60", "string", "String30", "string"),
("_c61", "string", "String31", "string"),
("_c62", "string", "String32", "string"),
("_c63", "string", "String33", "string"),
("_c64", "string", "String34", "string"),
("_c65", "string", "String35", "string"),
("_c66", "string", "String36", "string"),
("_c67", "string", "String37", "string"),
("_c68", "string", "String38", "string"),
("_c69", "string", "String39", "string"),
("_c70", "string", "String40", "string"),
("_c71", "string", "String41", "string"),
("_c72", "string", "String42", "string"),
("_c73", "string", "String43", "string"),
("_c74", "string", "String44", "string"),
("_c75", "string", "String45", "string"),
("_c76", "string", "String46", "string"),
("_c77", "string", "String47", "string"),
("_c78", "string", "String48", "string"),
("_c79", "string", "String49", "string"),
("_c80", "string", "String50", "string"),
("_c81", "int", "EBCampaignID", "int"),
("_c82", "long", "SECampaignID", "long"),
("_c83", "string", "SEAccountID", "string"),
("_c84", "long", "SEAdGroupID", "long"),
("_c85", "int", "DeviceTypeID", "int"),
("_c86", "int", "WinnerVersionID", "int"),
("_c87", "int", "WinnerTargetAudienceID", "int"),
("_c88", "int", "WinnerDeliveryGroupID", "int"),
("_c89", "boolean", "IsConversion", "boolean"),
("_c90", "string", "MobileDevice", "string"),
("_c91", "string", "WinnerPCP", "string"),
("_c92", "string", "WinnerEventID", "string"),
("_c93", "timestamp", "ConversionDateDefaultTImeZone", "timestamp"),
("_c94", "timestamp", "WinnerEventDateDefaultTImeZone", "timestamp"),
("_c95", "long", "UserIDNumeric", "long"),
("_c96", "int", "md_file_date", "int"),
("_c97", "long", "md_user_id_numeric", "long"),

("AcquiredTime", "timestamp", "AcquiredTime", "timestamp")
], transformation_ctx = "applymapping1")

## Drops all null fields in this DynamicFrame.
dropnullfields3 = DropNullFields.apply(frame = applymapping1, transformation_ctx = "dropnullfields3")
recordCount = dropnullfields3.count()

## Load data into Redshift
try:
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = "NewRedshiftConnection", connection_options =
        {
            "dbtable": tableName,
            "database": "wmprodfeeds"
        }, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")
    job.commit()
except:
    sendNotification(args, 'Fail',0,'','Error occurred while loading data into table')
    raise
try:
    print 'Delete Processed files'
    deleteProcessedFiles(args)
except:
    print 'Error while deleting files'
    raise
sendNotification(args, 'Successful',recordCount,tableName,'')