from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

import os


conf = SparkConf()\
    .setAppName('spark_s3')\
    .setMaster('local')

spark : SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()
sc : SparkContext = spark.sparkContext


df_new__schema = StructType(
    [
        StructField('id', IntegerType(), True),
        StructField('name', StringType(), True),
        StructField('is_legit', BooleanType(), True),
    ]
)

# create new DF using python's list of tuples
df_new:DataFrame = spark.createDataFrame(
    data=(
        (1,'john', True),
        (2, 'beth', False)
    ),
    schema=df_new__schema
)

df_new.printSchema()

# access the underlying RDDs thru the .rdd attributes
print(df_new.rdd.count())

# once RDDs are present, can operate on it like any other Dataset operations
print(df_new.rdd.filter(lambda row: row["id"]==1).count())

# ---------- PROGRAMME ENDS ----------
spark.stop()