import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# I/O options: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/io.html
df = spark.read.csv('data/sample_data.csv')

# Show a preview
df.show()

# Show preview of first / last n rows
df.head(5)
df.tail(5)

# Show preview as JSON (WARNING: in-memory)
df = df.limit(10) # optional
print(json.dumps([row.asDict(recursive=True) for row in df.collect()], indent=2))

# Limit actual DataFrame to n rows (non-deterministic)
df = df.limit(5)

# Get columns
df.columns

# Get columns + column types
df.dtypes

# Get schema
df.schema

# Get row count
df.count()

# Get column count
len(df.columns)

# Write output to disk
df.write.csv('data/output')

# Get results (WARNING: in-memory) as list of PySpark Rows
df = df.collect()

# # Get results (WARNING: in-memory) as list of Python dicts
# rows = df.collect()
# dicts = [row.asDict(recursive=True) for row in rows]

# # Convert (WARNING: in-memory) to Pandas DataFrame
# df = df.toPandas()