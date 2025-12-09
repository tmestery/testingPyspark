import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# I/O options: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/io.html
df = spark.read.csv('data/sample_data.csv')

# Show a preview
print("Show a preview: ")
df.show()

# Show preview of first / last n rows
print("Show preview of first / last n rows: ")
df.head(5)
df.tail(5)

# Show preview as JSON (WARNING: in-memory)
print("how preview as JSON (WARNING: in-memory): ")
df = df.limit(10) # optional
print(json.dumps([row.asDict(recursive=True) for row in df.collect()], indent=2))

# Limit actual DataFrame to n rows (non-deterministic)
print("Limit actual DataFrame to n rows (non-deterministic)")
df = df.limit(5)

# Get columns
print("Get columns: ")
df.columns

# Get columns + column types
print("Get columns + column types: ")
df.dtypes

# Get schema
print("Get schema: ")
df.schema

# Get row count
print("Get row count: ")
df.count()

# Get column count
print("Get column count: ")
len(df.columns)

# Write output to disk
print("Write output to disk: ")
df.write.csv('data/output')

# Get results (WARNING: in-memory) as list of PySpark Rows
print("Get results (WARNING: in-memory) as list of PySpark Rows: ")
df = df.collect()