import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Load data from AWS Glue Data Catalog
AWSGlueDataCatalog_node1739642261120 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_cleaned", 
    table_name="cleaned_statistics_reference_data", 
    transformation_ctx="AWSGlueDataCatalog_node1739642261120"
)

AWSGlueDataCatalog_node1739642413902 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_cleaned", 
    table_name="raw_statistics", 
    transformation_ctx="AWSGlueDataCatalog_node1739642413902"
)

# Log schemas of loaded data
print("Schema for cleaned_statistics_reference_data:")
AWSGlueDataCatalog_node1739642261120.printSchema()

print("Schema for raw_statistics:")
AWSGlueDataCatalog_node1739642413902.printSchema()

# Perform join operation
Join_node1739642432474 = Join.apply(
    frame1=AWSGlueDataCatalog_node1739642413902, 
    frame2=AWSGlueDataCatalog_node1739642261120, 
    keys1=["category_id"], 
    keys2=["id"], 
    transformation_ctx="Join_node1739642432474"
)

# Log schema of the joined data
print("Schema after join:")
Join_node1739642432474.printSchema()

# Verify presence of 'region' column
if 'region' not in Join_node1739642432474.toDF().columns:
    raise Exception("Partition column 'region' not found in the DataFrame schema")

# Evaluate data quality
EvaluateDataQuality().process_rows(
    frame=Join_node1739642432474, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739639774172", "enableDataQualityResultsPublishing": True}, 
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

# Write results to S3
AmazonS3_node1739642494978 = glueContext.getSink(
    path="s3://prashu-de-youtube-analytics-useast1-dev", 
    connection_type="s3", 
    updateBehavior="UPDATE_IN_DATABASE", 
    partitionKeys=["region", "category_id"], 
    enableUpdateCatalog=True, 
    transformation_ctx="AmazonS3_node1739642494978"
)

AmazonS3_node1739642494978.setCatalogInfo(catalogDatabase="db_youtube_analytics", catalogTableName="final_analytics")
AmazonS3_node1739642494978.setFormat("glueparquet", compression="snappy")
AmazonS3_node1739642494978.writeFrame(Join_node1739642432474)

job.commit()
