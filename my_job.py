from pyspark.sql import SparkSession
import random
from faker import Faker

# Initialize Faker
fake = Faker()

# Start Spark
spark = SparkSession.builder \
    .appName("RandomDataExample") \
    .getOrCreate()

# Generate 10,000 random records
data = []
departments = ["HR", "Finance", "IT", "Sales", "Marketing"]

for i in range(1, 10001):
    name = fake.first_name()
    dept = random.choice(departments)
    salary = random.randint(40000, 120000)
    data.append((i, name, dept, salary))

# Define schema
columns = ["id", "name", "department", "salary"]
print("Default partitions:", df.rdd.getNumPartitions())
# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show first 5 rows
df.show(5)

# Number of partitions
print("Default partitions:", df.rdd.getNumPartitions())

# Repartition into 8 partitions
df_repart = df.repartition(8)
print("After repartition(8):", df_repart.rdd.getNumPartitions())

# Save partitioned by department
df.write.partitionBy("department").mode("overwrite").parquet("output/employees_10k")

spark.stop()
