import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType

## unzip the bundle with all the required modules
import zipfile

zip_ref = zipfile.ZipFile('pythonmodules_glue.zip', 'r')
zip_ref.extractall('./tmp/packages')
zip_ref.close()
sys.path.insert(0, './tmp/packages')

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


## This function is used to send email notification to users.
def sendNotification(args, status):
    sender = args["sender_address"]
    toAddresses = args["recipient_address"]
    recipient = toAddresses.split(",")
    aws_region = "us-east-1"
    subject = "Glue job execution status: " + status
    # The email body for recipients with non-HTML email clients.
    if status == 'Successful':
        body_text = args["JOB_NAME"] + " executed successfully. "

        # The HTML body of the email.
        body_html = """<html>
        <head></head>
        <body>
          <h1>""" + args["JOB_NAME"] + """ executed successfully. </h1>
         </body>
        </html>"""
    else:
        body_text = (args["JOB_NAME"] + " failed. \r\n"
                                        "Please refer cloudwatch logs for details."
                     )

        # The HTML body of the email.
        body_html = """<html>
        <head></head>
        <body>
          <h1>""" + args["JOB_NAME"] + """ failed. </h1>
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

args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME','sender_address','recipient_address'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    s3path = "s3://dias.prod.us-east-1.dias-data/MEC_United_DFA2_Custom_Outbound_Daily_20180211/match/"
    databaseName = "wmprodfeeds"

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={
        "paths": [s3path + "activity_cats.csv"]}, format="csv")
    ds_activity_cats = myDyFrame.resolveChoice(
        specs=[('col0', 'cast:int'), ('col1', 'cast:int'), ('col2', 'cast:string'), ('col3', 'cast:int'),
               ('col4', 'cast:string'), ('col5', 'cast:string'), ('col6', 'cast:int')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3",
                                                              connection_options={"paths": [s3path + "activity_types.csv"]},
                                                              format="csv")
    ds_activity_types = myDyFrame.resolveChoice(
        specs=[('col0', 'cast:int'), ('col1', 'cast:int'), ('col2', 'cast:string'), ('col3', 'cast:string')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={
        "paths": [s3path + "ad_placement_assignments.csv"]}, format="csv")
    ds_ad_placement_assignments = myDyFrame.resolveChoice(specs=[('col0', 'cast:int'), ('col1', 'cast:int')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3",
                                                              connection_options={"paths": [s3path + "ads.csv"]},
                                                              format="csv")

    ds_ads = myDyFrame.resolveChoice(
        specs=[('col0', 'cast:int'), ('col1', 'cast:int'), ('col2', 'cast:int'), ('col3', 'cast:string'),
               ('col4', 'cast:string'), ('col5', 'cast:string'), ('col6', 'cast:string'), ('col5', 'cast:string')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3",
                                                              connection_options={"paths": [s3path + "advertisers.csv"]},
                                                              format="csv")

    ds_advertisers = myDyFrame.resolveChoice(
        specs=[('col0', 'cast:int'), ('col1', 'cast:int'), ('col2', 'cast:string'), ('col3', 'cast:int'),
               ('col4', 'cast:string')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3",
                                                              connection_options={"paths": [s3path + "browsers.csv"]},
                                                              format="csv")

    ds_browsers = myDyFrame.resolveChoice(specs=[('col0', 'cast:int'), ('col1', 'cast:string')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3",
                                                              connection_options={"paths": [s3path + "campaigns.csv"]},
                                                              format="csv")

    myFrame = myDyFrame.toDF().withColumn("col3",
                                          unix_timestamp("col3", "MM/dd/yyyy hh:mm:ss a").cast("timestamp")).withColumn(
        "col4", unix_timestamp("col4", "MM/dd/yyyy hh:mm:ss a").cast("timestamp"))
    myDyFrame = DynamicFrame.fromDF(myFrame, glueContext, "myDyFrame")
    ds_campaigns = myDyFrame.resolveChoice(
        specs=[('col0', 'cast:int'), ('col1', 'cast:int'), ('col2', 'cast:string'), ('col3', 'cast:date'),
               ('col4', 'cast:date'), ('col5', 'cast:string')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3",
                                                              connection_options={"paths": [s3path + "cities.csv"]},
                                                              format="csv")

    ds_cities = myDyFrame.resolveChoice(specs=[('col0', 'cast:int'), ('col1', 'cast:string')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={
        "paths": [s3path + "creative_ad_assignments.csv"]}, format="csv")

    ds_creative_ad_assignments = myDyFrame.resolveChoice(
        specs=[('col0', 'cast:int'), ('col1', 'cast:int'), ('col2', 'cast:long'), ('col3', 'cast:long'),
               ('col4', 'cast:string'), ('col5', 'cast:string'), ('col6', 'cast:string'), ('col7', 'cast:string')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3",
                                                              connection_options={"paths": [s3path + "creatives.csv"]},
                                                              format="csv")

    ds_creatives = myDyFrame.resolveChoice(
        specs=[('col0', 'cast:int'), ('col1', 'cast:int'), ('col2', 'cast:int'), ('col3', 'cast:string'),
               ('col4', 'cast:long'), ('col5', 'cast:string'), ('col6', 'cast:string'), ('col7', 'cast:string'),
               ('col8', 'cast:int')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={
        "paths": [s3path + "custom_creative_fields.csv"]}, format="csv")

    ds_custom_creative_fields = myDyFrame.resolveChoice(
        specs=[('col0', 'cast:int'), ('col1', 'cast:int'), ('col2', 'cast:int'), ('col3', 'cast:string'),
               ('col4', 'cast:string')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={
        "paths": [s3path + "custom_floodlight_variables.csv"]}, format="csv")

    ds_custom_floodlight_variables = myDyFrame.resolveChoice(
        specs=[('col0', 'cast:int'), ('col1', 'cast:string'), ('col2', 'cast:string')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={
        "paths": [s3path + "custom_rich_media.csv"]}, format="csv")

    ds_custom_rich_media = myDyFrame.resolveChoice(
        specs=[('col0', 'cast:int'), ('col1', 'cast:int'), ('col2', 'cast:string'), ('col3', 'cast:int'),
               ('col4', 'cast:string')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={
        "paths": [s3path + "designated_market_areas.csv"]}, format="csv")

    ds_designated_market_areas = myDyFrame.resolveChoice(specs=[('col0', 'cast:int'), ('col1', 'cast:string')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3",
                                                              connection_options={"paths": [s3path + "keyword_value.csv"]},
                                                              format="csv")

    ds_keyword_value = myDyFrame.resolveChoice(specs=[('col0', 'cast:int'), ('col1', 'cast:string')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={
        "paths": [s3path + "null_user_id_reason_categories.csv"]}, format="csv")

    ds_null_user_id_reason_categories = myDyFrame.resolveChoice(specs=[('col0', 'cast:int'), ('col1', 'cast:string')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={
        "paths": [s3path + "operating_systems.csv"]}, format="csv")

    ds_operating_systems = myDyFrame.resolveChoice(specs=[('col0', 'cast:int'), ('col1', 'cast:string')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3",
                                                              connection_options={"paths": [s3path + "paid_search.csv"]},
                                                              format="csv")

    ds_paid_search = myDyFrame.resolveChoice(
        specs=[('col0', 'cast:int'), ('col1', 'cast:int'), ('col2', 'cast:int'), ('col3', 'cast:long'),
               ('col4', 'cast:long'), ('col5', 'cast:long'), ('col6', 'cast:string'), ('col7', 'cast:string')])



    # myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3",
    #                                                           connection_options={"paths": [s3path + "placement_cost.csv"]},
    #                                                           format="csv")
    # myFrame = myDyFrame.toDF().withColumn("col1",
    #                                       unix_timestamp("col1", "MM/dd/yyyy hh:mm:ss a").cast("timestamp")).withColumn(
    #     "col2", unix_timestamp("col2", "MM/dd/yyyy hh:mm:ss a").cast("timestamp"))
    # myDyFrame = DynamicFrame.fromDF(myFrame, glueContext, "myDyFrame")
    # ds_placement_cost = myDyFrame.resolveChoice(
    #     specs=[('col0', 'cast:int'), ('col1', 'cast:date'), ('col2', 'cast:date'), ('col3', 'cast:long'),
    #            ('col4', 'cast:double'), ('col5', 'cast:string')])




    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3",
                                                              connection_options={"paths": [s3path + "placements.csv"]},
                                                              format="csv")

    myFrame = myDyFrame.toDF().withColumn("col7",
                                          unix_timestamp("col7", "MM/dd/yyyy hh:mm:ss a").cast("timestamp")).withColumn(
        "col8", unix_timestamp("col8", "MM/dd/yyyy hh:mm:ss a").cast("timestamp"))
    myDyFrame = DynamicFrame.fromDF(myFrame, glueContext, "myDyFrame")
    ds_placements = myDyFrame.resolveChoice(
        specs=[('col0', 'cast:int'), ('col1', 'cast:int'), ('col2', 'cast:int'), ('col3', 'cast:string'),
               ('col4', 'cast:string'), ('col5', 'cast:string'), ('col6', 'cast:string'), ('col7', 'cast:date'),
               ('col8', 'cast:date'), ('col9', 'cast:string'), ('col10', 'cast:string'), ('col11', 'cast:string'),
               ('col12', 'cast:string'), ('col13', 'cast:int'), ('col14', 'cast:boolean')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={
        "paths": [s3path + "rich_media_standard_event_types.csv"]}, format="csv")

    ds_rich_media_standard_event_types = myDyFrame.resolveChoice(specs=[('col0', 'cast:int'), ('col1', 'cast:string')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3",
                                                              connection_options={"paths": [s3path + "sites.csv"]},
                                                              format="csv")

    ds_sites = myDyFrame.resolveChoice(
        specs=[('col0', 'cast:int'), ('col1', 'cast:string'), ('col2', 'cast:int'), ('col3', 'cast:string')])

    myDyFrame = glueContext.create_dynamic_frame_from_options(connection_type="s3",
                                                              connection_options={"paths": [s3path + "states.csv"]},
                                                              format="csv")

    ds_states = myDyFrame.resolveChoice(specs=[('col0', 'cast:string'), ('col1', 'cast:string')])

    am_activity_cats = ApplyMapping.apply(frame=ds_activity_cats,
                                          mappings=[("col0", "int", "floodlight_configuration", "int"),
                                                    ("col1", "int", "activity_group_id", "int"),
                                                    ("col2", "string", "activity_type", "string"),
                                                    ("col3", "int", "activity_id", "int"),
                                                    ("col4", "string", "activity_sub_type", "string"),
                                                    ("col5", "string", "activity", "string"),
                                                    ("col6", "int", "tag_counting_method_id", "int")],
                                          transformation_ctx="am_activity_cats")
    am_activity_types = ApplyMapping.apply(frame=ds_activity_types,
                                           mappings=[("col0", "int", "floodlight_configuration", "int"),
                                                     ("col1", "int", "activity_group_id", "int"),
                                                     ("col2", "string", "activity_type", "string"),
                                                     ("col3", "string", "activity_group", "string")],
                                           transformation_ctx="am_activity_types")
    am_advertisers = ApplyMapping.apply(frame=ds_advertisers, mappings=[("col0", "int", "floodlight_configuration", "int"),
                                                                        ("col1", "int", "advertiser_id", "int"),
                                                                        ("col2", "string", "advertiser", "string"),
                                                                        ("col3", "int", "advertiser_group_id", "int"),
                                                                        ("col4", "string", "advertiser_group", "string")],
                                        transformation_ctx="am_advertisers")
    am_ads = ApplyMapping.apply(frame=ds_ads, mappings=[("col0", "int", "advertiser_id", "int"),
                                                        ("col1", "int", "campaign_id", "int"),
                                                        ("col2", "int", "ad_id", "int"), ("col3", "string", "ad", "string"),
                                                        ("col4", "string", "ad_click_url", "string"),
                                                        ("col5", "string", "ad_type", "string"),
                                                        ("col6", "string", "creative_pixel_size", "string"),
                                                        ("col7", "string", "ad_comments", "string")],
                                transformation_ctx="am_ads")
    am_ad_placement_assignments = ApplyMapping.apply(frame=ds_ad_placement_assignments,
                                                     mappings=[("col0", "int", "ad_id", "int"),
                                                               ("col1", "int", "placement_id", "int")],
                                                     transformation_ctx="am_ad_placement_assignments")
    am_browsers = ApplyMapping.apply(frame=ds_browsers, mappings=[("col0", "int", "browser_platform_id", "int"),
                                                                  ("col1", "string", "browser_platform", "string")],
                                     transformation_ctx="am_browsers")
    am_campaigns = ApplyMapping.apply(frame=ds_campaigns, mappings=[("col0", "int", "advertiser_id", "int"),
                                                                    ("col1", "int", "campaign_id", "int"),
                                                                    ("col2", "string", "campaign", "string"),
                                                                    ("col3", "date", "campaign_start_date", "date"),
                                                                    ("col4", "date", "campaign_end_date", "date"),
                                                                    ("col5", "string", "billing_invoice_code", "string")],
                                      transformation_ctx="am_campaigns")
    am_cities = ApplyMapping.apply(frame=ds_cities, mappings=[("col0", "int", "city_id", "int"),
                                                              ("col1", "string", "city", "string")],
                                   transformation_ctx="am_cities")
    am_creative_ad_assignments = ApplyMapping.apply(frame=ds_creative_ad_assignments,
                                                    mappings=[("col0", "int", "ad_id", "int"),
                                                              ("col1", "int", "creative_id", "int"),
                                                              ("col2", "long", "creative_start_date", "long"),
                                                              ("col3", "long", "creative_end_date", "long"),
                                                              ("col4", "string", "creative_rotation_type", "string"),
                                                              ("col5", "string", "creative_groups_1", "string"),
                                                              ("col6", "string", "creative_groups_2", "string"),
                                                              ("col7", "string", "ad_click_url", "string")],
                                                    transformation_ctx="am_creative_ad_assignments")
    am_creatives = ApplyMapping.apply(frame=ds_creatives, mappings=[("col0", "int", "advertiser_id", "int"),
                                                                  ("col1", "int", "rendering_id", "int"),
                                                                  ("col2", "int", "creative_id", "int"),
                                                                  ("col3", "string", "creative", "string"),
                                                                  ("col4", "long", "creative_last_modified_date", "long"),
                                                                  ("col5", "string", "creative_type", "string"),
                                                                  ("col6", "string", "creative_pixel_size", "string"),
                                                                  ("col7", "string", "creative_image_url", "string"),
                                                                  ("col8", "int", "creative_version", "int")],
                                     transformation_ctx="am_creatives")
    am_custom_creative_fields = ApplyMapping.apply(frame=ds_custom_creative_fields, mappings=[
        ("col0", "int", "advertiser_id", "int"), ("col1", "int", "creative_id", "int"),
        ("col2", "int", "creative_field_number", "int"), ("col3", "string", "creative_field_name", "string"),
        ("col4", "string", "creative_field_value", "string")], transformation_ctx="am_custom_creative_fields")
    am_custom_floodlight_variables = ApplyMapping.apply(frame=ds_custom_floodlight_variables, mappings=[
        ("col0", "int", "floodlight_configuration", "int"), ("col1", "string", "floodlight_variable_id", "string"),
        ("col2", "string", "floodlight_variable_name", "string")], transformation_ctx="am_custom_floodlight_variables")
    am_custom_rich_media = ApplyMapping.apply(frame=ds_custom_rich_media, mappings=[("col0", "int", "advertiser_id", "int"),
                                                                                    ("col1", "int", "rich_media_event_id",
                                                                                     "int"), (
                                                                                        "col2", "string", "rich_media_event",
                                                                                        "string"),
                                                                                    ("col3", "int",
                                                                                     "rich_media_event_type_id", "int"), (
                                                                                        "col4", "string",
                                                                                        "rich_media_event_type", "string")],
                                              transformation_ctx="am_custom_rich_media")
    am_designated_market_areas = ApplyMapping.apply(frame=ds_designated_market_areas, mappings=[
        ("col0", "int", "designated_market_area_dma_id", "int"), ("col1", "string", "designated_market_area", "string")],
                                                    transformation_ctx="am_designated_market_areas")
    am_keyword_value = ApplyMapping.apply(frame=ds_keyword_value, mappings=[("col0", "int", "ad_id", "int"),
                                                                            ("col1", "string", "keyword", "string")],
                                          transformation_ctx="am_keyword_value")
    am_null_user_id_reason_categories = ApplyMapping.apply(frame=ds_null_user_id_reason_categories, mappings=[
        ("col0", "int", "id", "int"), ("col1", "string", "reason_category", "string")],
                                                           transformation_ctx="am_null_user_id_reason_categories")
    am_operating_systems = ApplyMapping.apply(frame=ds_operating_systems,
                                              mappings=[("col0", "int", "operating_system_id", "int"),
                                                        ("col1", "string", "operating_system", "string")],
                                              transformation_ctx="am_operating_systems")
    am_paid_search = ApplyMapping.apply(frame=ds_paid_search, mappings=[("col0", "int", "ad_id", "int"),
                                                                        ("col1", "int", "advertiser_id", "int"),
                                                                        ("col2", "int", "campaign_id", "int"),
                                                                        ("col3", "long", "paid_search_ad_id", "long"), (
                                                                            "col4", "long", "paid_search_legacy_keyword_id",
                                                                            "long"),
                                                                        ("col5", "long", "paid_search_keyword_id", "long"),
                                                                        (
                                                                            "col6", "string", "paid_search_campaign", "string"),
                                                                        (
                                                                            "col7", "string", "paid_search_ad_group", "string"),
                                                                        ("col8", "string", "paid_search_bid_strategy",
                                                                         "string"),
                                                                        ("col9", "string", "paid_search_landing_page_url",
                                                                         "string"), (
                                                                            "col10", "string", "paid_search_keyword", "string"),
                                                                        ("col11", "string", "paid_search_match_type",
                                                                         "string")], transformation_ctx="am_paid_search")




    # am_placement_cost = ApplyMapping.apply(frame=ds_placement_cost, mappings=[("col0", "int", "placement_id", "int"),
    #                                                                           ("col1", "date", "placement_start_date",
    #                                                                            "date"), (
    #                                                                               "col2", "date", "placement_end_date", "date"),
    #                                                                           ("col3", "long", "package_total_booked_units",
    #                                                                            "long"), (
    #                                                                               "col4", "double", "placement_rate", "double"),
    #                                                                           ("col5", "string", "placement_comments",
    #                                                                            "string")],
    #                                        transformation_ctx="am_placement_cost")




    am_placements = ApplyMapping.apply(frame=ds_placements, mappings=[("col0", "int", "campaign_id", "int"),
                                                                      ("col1", "int", "site_id_dcm", "int"),
                                                                      ("col2", "int", "placement_id", "int"),
                                                                      ("col3", "string", "site_keyname", "string"),
                                                                      ("col4", "string", "placement", "string"),
                                                                      ("col5", "string", "content_category", "string"),
                                                                      ("col6", "string", "placement_strategy", "string"),
                                                                      ("col7", "date", "placement_start_date", "date"),
                                                                      ("col8", "date", "placement_end_date", "date"),
                                                                      ("col9", "string", "placement_group_type", "string"),
                                                                      ("col10", "string", "package_roadblock_id", "string"),
                                                                      ("col11", "string", "placement_cost_structure",
                                                                       "string"), (
                                                                          "col12", "string", "placement_cap_cost_option",
                                                                          "string"),
                                                                      ("col13", "int", "activity_id", "int"), (
                                                                          "col14", "boolean", "flighting_activated",
                                                                          "boolean")], transformation_ctx="am_placements")
    am_rich_media_standard_event_types = ApplyMapping.apply(frame=ds_rich_media_standard_event_types, mappings=[
        ("col0", "int", "id", "int"), ("col1", "string", "name", "string")],
                                                            transformation_ctx="am_rich_media_standard_event_types")
    am_sites = ApplyMapping.apply(frame=ds_sites, mappings=[("col0", "int", "site_id_dcm", "int"),
                                                            ("col1", "string", "site_dcm", "string"),
                                                            ("col2", "int", "site_id_site_directory", "int"),
                                                            ("col3", "string", "site_site_directory", "string")],
                                  transformation_ctx="am_sites")
    am_states = ApplyMapping.apply(frame=ds_states, mappings=[("col0", "string", "state_region", "string"),
                                                              ("col1", "string", "state_region_full_name", "string")],
                                   transformation_ctx="am_states")

    advertisers = DropNullFields.apply(frame=am_advertisers, transformation_ctx="advertisers")
    ads = DropNullFields.apply(frame=am_ads, transformation_ctx="ads")
    activity_cats = DropNullFields.apply(frame=am_activity_cats, transformation_ctx="activity_cats")
    activity_types = DropNullFields.apply(frame=am_activity_types, transformation_ctx="activity_types")
    ad_placement_assignments = DropNullFields.apply(frame=am_ad_placement_assignments,
                                                    transformation_ctx="ad_placement_assignments")
    browsers = DropNullFields.apply(frame=am_browsers, transformation_ctx="browsers")
    campaigns = DropNullFields.apply(frame=am_campaigns, transformation_ctx="campaigns")
    cities = DropNullFields.apply(frame=am_cities, transformation_ctx="cities")
    creative_ad_assignments = DropNullFields.apply(frame=am_creative_ad_assignments,
                                                   transformation_ctx="creative_ad_assignments")
    creatives = DropNullFields.apply(frame=am_creatives, transformation_ctx="creatives")
    custom_creative_fields = DropNullFields.apply(frame=am_custom_creative_fields,
                                                  transformation_ctx="custom_creative_fields")
    custom_floodlight_variables = DropNullFields.apply(frame=am_custom_floodlight_variables,
                                                       transformation_ctx="custom_floodlight_variables")
    custom_rich_media = DropNullFields.apply(frame=am_custom_rich_media, transformation_ctx="custom_rich_media")
    designated_market_areas = DropNullFields.apply(frame=am_designated_market_areas,
                                                   transformation_ctx="designated_market_areas")
    keyword_value = DropNullFields.apply(frame=am_keyword_value, transformation_ctx="keyword_value")
    null_user_id_reason_categories = DropNullFields.apply(frame=am_null_user_id_reason_categories,
                                                          transformation_ctx="null_user_id_reason_categories")
    operating_systems = DropNullFields.apply(frame=am_operating_systems, transformation_ctx="operating_systems")
    paid_search = DropNullFields.apply(frame=am_paid_search, transformation_ctx="paid_search")



    # placement_cost = DropNullFields.apply(frame=am_placement_cost, transformation_ctx="placement_cost")



    placements = DropNullFields.apply(frame=am_placements, transformation_ctx="placements")
    rich_media_standard_event_types = DropNullFields.apply(frame=am_rich_media_standard_event_types,
                                                           transformation_ctx="rich_media_standard_event_types")
    sites = DropNullFields.apply(frame=am_sites, transformation_ctx="sites")
    states = DropNullFields.apply(frame=am_states, transformation_ctx="states")

    datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=advertisers, catalog_connection="NewRedshiftConnection",
                                                               connection_options={
                                                                   "dbtable": "united.DFA2_advertisers",
                                                                   "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                               transformation_ctx="datasink1")
    datasink2 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=ads, catalog_connection="NewRedshiftConnection",
                                                               connection_options={
                                                                   "dbtable": "united.DFA2_ads",
                                                                   "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                               transformation_ctx="datasink2")
    datasink3 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=activity_cats,
                                                               catalog_connection="NewRedshiftConnection",
                                                               connection_options={
                                                                   "dbtable": "united.DFA2_activity_cats",
                                                                   "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                               transformation_ctx="datasink3")
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=activity_types,
                                                               catalog_connection="NewRedshiftConnection",
                                                               connection_options={
                                                                   "dbtable": "united.DFA2_activity_types",
                                                                   "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                               transformation_ctx="datasink4")
    datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=ad_placement_assignments,
                                                               catalog_connection="NewRedshiftConnection",
                                                               connection_options={
                                                                   "dbtable": "united.DFA2_ad_placement_assignments",
                                                                   "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                               transformation_ctx="datasink5")
    datasink6 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=browsers, catalog_connection="NewRedshiftConnection",
                                                               connection_options={
                                                                   "dbtable": "united.DFA2_browsers",
                                                                   "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                               transformation_ctx="datasink6")
    datasink7 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=campaigns, catalog_connection="NewRedshiftConnection",
                                                               connection_options={
                                                                   "dbtable": "united.DFA2_campaigns",
                                                                   "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                               transformation_ctx="datasink7")
    datasink8 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=cities, catalog_connection="NewRedshiftConnection",
                                                               connection_options={
                                                                   "dbtable": "united.DFA2_cities",
                                                                   "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                               transformation_ctx="datasink8")
    datasink9 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=creative_ad_assignments,
                                                               catalog_connection="NewRedshiftConnection",
                                                               connection_options={
                                                                   "dbtable": "united.DFA2_creative_ad_assignments",
                                                                   "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                               transformation_ctx="datasink9")
    datasink10 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=creatives, catalog_connection="NewRedshiftConnection",
                                                                connection_options={
                                                                    "dbtable": "united.DFA2_creatives",
                                                                    "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                                transformation_ctx="datasink10")
    datasink11 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=custom_creative_fields,
                                                                catalog_connection="NewRedshiftConnection",
                                                                connection_options={
                                                                    "dbtable": "united.DFA2_custom_creative_fields",
                                                                    "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                                transformation_ctx="datasink11")
    datasink12 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=custom_floodlight_variables,
                                                                catalog_connection="NewRedshiftConnection",
                                                                connection_options={
                                                                    "dbtable": "united.DFA2_custom_floodlight_variables",
                                                                    "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                                transformation_ctx="datasink12")
    datasink13 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=custom_rich_media,
                                                                catalog_connection="NewRedshiftConnection",
                                                                connection_options={
                                                                    "dbtable": "united.DFA2_custom_rich_media",
                                                                    "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                                transformation_ctx="datasink13")
    datasink14 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=designated_market_areas,
                                                                catalog_connection="NewRedshiftConnection",
                                                                connection_options={
                                                                    "dbtable": "united.DFA2_designated_market_areas",
                                                                    "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                                transformation_ctx="datasink14")
    datasink15 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=keyword_value,
                                                                catalog_connection="NewRedshiftConnection",
                                                                connection_options={
                                                                    "dbtable": "united.DFA2_keyword_value",
                                                                    "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                                transformation_ctx="datasink15")
    datasink16 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=null_user_id_reason_categories,
                                                                catalog_connection="NewRedshiftConnection",
                                                                connection_options={
                                                                    "dbtable": "united.DFA2_null_user_id_reason_categories",
                                                                    "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                                transformation_ctx="datasink16")
    datasink17 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=operating_systems,
                                                                catalog_connection="NewRedshiftConnection",
                                                                connection_options={
                                                                    "dbtable": "united.DFA2_operating_systems",
                                                                    "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                                transformation_ctx="datasink17")
    datasink18 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=paid_search,
                                                                catalog_connection="NewRedshiftConnection",
                                                                connection_options={
                                                                    "dbtable": "united.DFA2_paid_search",
                                                                    "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                                transformation_ctx="datasink18")



    # datasink19 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=placement_cost,
    #                                                             catalog_connection="NewRedshiftConnection",
    #                                                             connection_options={
    #                                                                 "dbtable": "united.DFA2_placement_cost",
    #                                                                 "database": databaseName}, redshift_tmp_dir=args["TempDir"],
    #                                                             transformation_ctx="datasink19")



    datasink20 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=placements, catalog_connection="NewRedshiftConnection",
                                                                connection_options={
                                                                    "dbtable": "united.DFA2_placements",
                                                                    "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                                transformation_ctx="datasink20")
    datasink21 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=rich_media_standard_event_types,
                                                                catalog_connection="NewRedshiftConnection",
                                                                connection_options={
                                                                    "dbtable": "united.DFA2_rich_media_standard_event_types",
                                                                    "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                                transformation_ctx="datasink21")
    datasink22 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=sites, catalog_connection="NewRedshiftConnection",
                                                                connection_options={
                                                                    "dbtable": "united.DFA2_sites",
                                                                    "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                                transformation_ctx="datasink22")
    datasink23 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=states, catalog_connection="NewRedshiftConnection",
                                                                connection_options={
                                                                    "dbtable": "united.DFA2_states",
                                                                    "database": databaseName}, redshift_tmp_dir=args["TempDir"],
                                                                transformation_ctx="datasink23")

    job.commit()
    sendNotification(args, 'Successful')
except:
    sendNotification(args, 'Fail')
    raise