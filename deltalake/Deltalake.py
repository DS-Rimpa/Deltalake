'''
%%tags
{
    "":""
}
'''

import sys
import timeit
import logging
import pyspark

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import multiprocessing
import pandas
import concurrent.futures as cf
from awsglue.dynamicframe import DynamicFrame
glueContext = GlueContext(SparkContext.getOrCreate())
sparksession = glueContext.spark_session

conf = (
    pyspark.SparkConf()
        .setAppName('poc_deltalake')
        .set("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .set('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
  		#Configuring Catalog
        .set('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
)
spark = sparksession.builder.config(conf=conf).getOrCreate()
print("Spark Running")
inputDynamicframe = glueContext.create_dynamic_frame_from_options(connection_type ="s3",connection_options = {"paths": ["s3://lc-aug1-2023/source/"], "recurse": True },format = "csv", format_options={"withHeader": True}, transformation_ctx ="inputDynamicframe")
inputDf = inputDynamicframe.toDF()
inputDf.show()
deltalake_current_options = {
    "path": "s3://lc-aug1-2023/deltalake/deltalake-table/current/"
}
inputDf.write.format("delta").options(**deltalake_current_options).mode("overwrite").save()
'''
inputDynamicframeUpdate = glueContext.create_dynamic_frame_from_options(connection_type ="s3",connection_options = {"paths": ["s3://lc-aug1-2023/update/"], "recurse": True },format = "csv", format_options={"withHeader": True}, transformation_ctx ="inputDynamicframeUpdate")
inputDfUpdate = inputDynamicframeUpdate.toDF()
inputDfUpdate.show()
deltalake_update_options = {
    "path": "s3://lc-aug1-2023/deltalake/deltalake-table/update/"
}
inputDfUpdate.write.format("delta").options(**deltalake_update_options).mode("overwrite").save()
'''
'''
deltalake_current_options = {
    "path": "s3://lc-aug1-2023/deltalake/deltalake-table/update/"
}
'''
dataFrameTarget = spark.read.format("delta").option("header", "true").load(**deltalake_current_options)
dataFrameTarget.show()

target_path = "s3://lc-aug1-2023/deltalake/target/"
dataFrameTarget.repartition(1).write.mode('overwrite').csv(target_path, header=True)
job.commit()