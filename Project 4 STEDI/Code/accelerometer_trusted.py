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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1685236691138 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="accelerometer_landing_proj",
    transformation_ctx="AWSGlueDataCatalog_node1685236691138",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1685237141277 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="customer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1685237141277",
)

# Script generated for node Join
Join_node1685235496976 = Join.apply(
    frame1=AWSGlueDataCatalog_node1685236691138,
    frame2=AWSGlueDataCatalog_node1685237141277,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1685235496976",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=Join_node1685235496976,
    mappings=[
        ("user", "string", "user", "string"),
        ("timestamp", "long", "timestamp", "long"),
        ("x", "float", "x", "double"),
        ("y", "float", "y", "double"),
        ("z", "float", "z", "double"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-wjb/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
