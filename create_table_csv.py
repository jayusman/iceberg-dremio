#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import os

## DEFINE SENSITIVE VARIABLES
CATALOG_URI = "http://nessie:19120/api/v1"  # Nessie Server URI
WAREHOUSE = "s3://warehouse/"               # Minio Address to Write to
STORAGE_URI = "http://172.21.0.3:9000"      # Minio IP address from docker inspect
CSV_PATH = "/workspace/seed-data/employees.csv"  # Path Files CSV

# Configure Spark with necessary packages, Iceberg/Nessie settings, and logging
conf = (
    pyspark.SparkConf()
        .setAppName("employees_data_app")
        .setMaster("local[*]")  # Runs locally with all available cores
        # Enable logging for Spark History Server
        .set("spark.eventLog.enabled", "true")
        .set("spark.eventLog.dir", "file:///tmp/spark-events")
        .set("spark.history.fs.logDirectory", "file:///tmp/spark-events")
        # Include necessary packages
        .set("spark.jars.packages", "org.postgresql:postgresql:42.7.3,"
                                     "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                                     "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
                                     "software.amazon.awssdk:bundle:2.24.8,"
                                     "software.amazon.awssdk:url-connection-client:2.24.8")
        # Enable Iceberg and Nessie extensions
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                                      "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        # Configure Nessie catalog
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.nessie.uri", CATALOG_URI)
        .set("spark.sql.catalog.nessie.ref", "main")
        .set("spark.sql.catalog.nessie.authentication.type", "NONE")
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        # Set Minio as the S3 endpoint for Iceberg storage
        .set("spark.sql.catalog.nessie.s3.endpoint", STORAGE_URI)
        .set("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
        .set("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
)

# Start Spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("🚀 Spark Session Started")

# Define schema based on CSV headers
schema = StructType([
    StructField("EMPLOYEE_ID", IntegerType(), True),
    StructField("FIRST_NAME", StringType(), True),
    StructField("LAST_NAME", StringType(), True),
    StructField("EMAIL", StringType(), True),
    StructField("PHONE_NUMBER", StringType(), True),
    StructField("HIRE_DATE", StringType(), True),
    StructField("JOB_ID", StringType(), True),
    StructField("SALARY", IntegerType(), True),
    StructField("COMMISSION_PCT", StringType(), True),
    StructField("MANAGER_ID", IntegerType(), True),
    StructField("DEPARTMENT_ID", IntegerType(), True)
])

# Create Iceberg namespace
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.employees")

# Read CSV with schema
df = spark.read.csv(CSV_PATH, schema=schema, header=True)
df.show()

# Create Iceberg table
df.writeTo("nessie.employees.employees_data_raw").createOrReplace()

# Verify by reading from Iceberg table
df_read = spark.read.format("iceberg").load("nessie.employees.employees_data_raw")
df_read.show()

# Stop Spark session
spark.stop()
print("✅ Spark Session Stopped")


# In[ ]:




