import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-wjb/step-trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1685240102581 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="customer_curated",
    transformation_ctx="AWSGlueDataCatalog_node1685240102581",
)

# Script generated for node Join
Join_node1685240118673 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AWSGlueDataCatalog_node1685240102581,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1685240118673",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=Join_node1685240118673,
    mappings=[
        ("sensorReadingTime", "long", "sensorReadingTime", "long"),
        ("serialNumber", "string", "serialNumber", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "int"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-wjb/step-trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
