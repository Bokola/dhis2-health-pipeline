from pyspark.sql import SparkSession

# initialize spark session
spark = SparkSession.builder \
    .appName("InspectCompleteness") \
    .master("local[*]") \
    .getOrCreate()

# read the completeness aggregation csv
df = spark.read.option("header", "true").option("inferSchema", "true").csv("./output/aggregations/country_completeness.csv")

# show the schema and data rows
print("\n--- completeness schema ---")
df.printSchema()

print("--- completeness data sample ---")
df.show(20, truncate=False)
