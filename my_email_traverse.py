# -*- coding: utf-8 -*-
"""
Created on Thu Aug 21 20:27:51 2025

@author: vij_c
"""
from pyspark.sql import SparkSession

# Start Spark

spark = SparkSession.builder \
    .appName("ParquetExample") \
           .getOrCreate()

# Sample DataFrame
data = [
    (Alice, "7038331668", "vijay1@yahoo.com","dj@yahoo.com "),
     (klice, "7038331668", "vijay2@yahoo.com"," "),
     (Alice2, "", "vijay2@yahoo.com"," ")
	  (Alice3, "", "","vijay3@yahoo.com ")
]
columns = ["Name", "phone", "email1", "email2"]

df = spark.createDataFrame(data, columns)

# Write DataFrame to Parquet
df.write.mode("overwrite").parquet("output/customers.parquet")
print("✅ Parquet file written to output/customers.parquet")

# Read it back
parquet_df = spark.read.parquet("output/customers.parquet")
parquet_df.show()