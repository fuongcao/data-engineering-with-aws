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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1714122047373 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1714122047373")

# Script generated for node Customer Trusted
CustomerTrusted_node1714122073977 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1714122073977")

# Script generated for node Research Sharing Agreement
ResearchSharingAgreement_node1714122106613 = Join.apply(frame1=AccelerometerLanding_node1714122047373, frame2=CustomerTrusted_node1714122073977, keys1=["user"], keys2=["email"], transformation_ctx="ResearchSharingAgreement_node1714122106613")

# Script generated for node Drop Fields
DropFields_node1714122140596 = DropFields.apply(frame=ResearchSharingAgreement_node1714122106613, paths=["email", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate", "customername"], transformation_ctx="DropFields_node1714122140596")

# Script generated for node Drop Duplicates
DropDuplicates_node1714124291595 =  DynamicFrame.fromDF(DropFields_node1714122140596.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1714124291595")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1714122162710 = glueContext.getSink(path="s3://prj3-stedi/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1714122162710")
AccelerometerTrusted_node1714122162710.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1714122162710.setFormat("json")
AccelerometerTrusted_node1714122162710.writeFrame(DropDuplicates_node1714124291595)
job.commit()