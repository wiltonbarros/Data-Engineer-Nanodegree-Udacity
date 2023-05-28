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
AWSGlueDataCatalog_node1685242483601 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="step-trainer-trusted",
    transformation_ctx="AWSGlueDataCatalog_node1685242483601",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1685242504450 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="accelerometer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1685242504450",
)

# Script generated for node Join
Join_node1685242546096 = Join.apply(
    frame1=AWSGlueDataCatalog_node1685242483601,
    frame2=AWSGlueDataCatalog_node1685242504450,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1685242546096",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=Join_node1685242546096,
    mappings=[
        ("sensorreadingtime", "long", "sensorreadingtime", "long"),
        ("serialnumber", "string", "serialnumber", "string"),
        ("distancefromobject", "int", "distancefromobject", "int"),
        ("x", "float", "x", "float"),
        ("y", "float", "y", "float"),
        ("z", "float", "z", "float"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-wjb/machine-learning-curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
