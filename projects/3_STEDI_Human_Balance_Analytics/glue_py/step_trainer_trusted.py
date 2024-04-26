import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Curated
CustomerCurated_node1714143929092 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1714143929092")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1714143906255 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1714143906255")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT stp.serialnumber, stp.sensorreadingtime, stp.distancefromobject
FROM step_trainer_landing stp
    INNER JOIN customer_curated cus ON stp.serialnumber = cus.serialnumber
'''
SQLQuery_node1714146439671 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_landing":StepTrainerLanding_node1714143906255, "customer_curated":CustomerCurated_node1714143929092}, transformation_ctx = "SQLQuery_node1714146439671")

# Script generated for node Drop Duplicates
DropDuplicates_node1714144288404 =  DynamicFrame.fromDF(SQLQuery_node1714146439671.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1714144288404")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1714144298602 = glueContext.getSink(path="s3://prj3-stedi/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1714144298602")
StepTrainerTrusted_node1714144298602.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1714144298602.setFormat("json")
StepTrainerTrusted_node1714144298602.writeFrame(DropDuplicates_node1714144288404)
job.commit()