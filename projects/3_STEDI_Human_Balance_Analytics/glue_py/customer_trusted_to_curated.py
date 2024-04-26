import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1714125056159 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1714125056159")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1714125086905 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1714125086905")

# Script generated for node Join
Join_node1714126801268 = Join.apply(frame1=CustomerTrusted_node1714125056159, frame2=AccelerometerLanding_node1714125086905, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1714126801268")

# Script generated for node Drop Fields
DropFields_node1714126866612 = DropFields.apply(frame=Join_node1714126801268, paths=["user", "timestamp", "x", "y", "z"], transformation_ctx="DropFields_node1714126866612")

# Script generated for node Drop Duplicates
DropDuplicates_node1714126891978 =  DynamicFrame.fromDF(DropFields_node1714126866612.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1714126891978")

# Script generated for node Customer Curated
CustomerCurated_node1714125184465 = glueContext.getSink(path="s3://prj3-stedi/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1714125184465")
CustomerCurated_node1714125184465.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1714125184465.setFormat("json")
CustomerCurated_node1714125184465.writeFrame(DropDuplicates_node1714126891978)
job.commit()