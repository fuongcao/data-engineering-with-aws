import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1714148108694 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1714148108694")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1714147039869 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1714147039869")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT stp.sensorreadingtime, stp.serialnumber, stp.distancefromobject,
    acc.timestamp, acc.x, acc.y, acc.z
    FROM step_trainer_trusted stp
    JOIN accelerometer_trusted acc ON stp.sensorreadingtime = acc.timestamp
'''
SQLQuery_node1714148169873 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":StepTrainerTrusted_node1714147039869, "accelerometer_trusted":AccelerometerTrusted_node1714148108694}, transformation_ctx = "SQLQuery_node1714148169873")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1714148229110 = glueContext.getSink(path="s3://prj3-stedi/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1714148229110")
MachineLearningCurated_node1714148229110.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1714148229110.setFormat("json")
MachineLearningCurated_node1714148229110.writeFrame(SQLQuery_node1714148169873)
job.commit()