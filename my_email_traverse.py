# -*- coding: utf-8 -*-
"""
Created on Thu Aug 21 20:27:51 2025

@author: vij_c
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark

spark = SparkSession.builder \
    .appName("ParquetExample") \
           .getOrCreate()

# Sample DataFrame
data = [
    ("Alice", "7038331668", "vijay1@yahoo.com","dj@yahoo.com "),
     ("klice", "7038331668", "vijay2@yahoo.com"," "),
     ("Alice2", "", "vijay2@yahoo.com","ssss "),
	  ("Alice3", "", "","vijay3@yahoo.com "),
      ("Alice5", "", "","")
]
columns = ["Name", "phone", "email1", "email2"]

df = spark.createDataFrame(data, columns)
df_filtered = df.filter(col("phone").isNotNull() & col("email1").isNotNull() & col("email2").isNotNull() )

df_filtered.show()