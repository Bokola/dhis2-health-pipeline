from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# initialize native local spark instance
spark = SparkSession.builder \
    .appName("ReportingRateAudit") \
    .master("local[*]") \
    .getOrCreate()

# exact target path uncovered in the directory dump
output_path = "./output/analytics/country_reporting_rates.parquet"

print(f"\n--- auditing target dataset: {output_path} ---")

try:
    # read the specific target parquet directory directly
    df = spark.read.parquet(output_path)
    
    # print schema layout to confirm columns
    print("\n--- schema layout ---")
    df.printSchema()
    
    # locate the exact column names used for reporting rate calculations
    # dynamically fallback to whatever reporting column name exists
    cols = df.columns
    rate_col = next((c for c in cols if "rate" in c.lower() or "completeness" in c.lower()), None)
    
    if not rate_col:
        print(f"could not automatically identify rate column. available columns: {cols}")
        print("showing raw dataset snapshot:")
        df.show(10, truncate=False)
    else:
        print(f"identified metrics column: {rate_col}")
        
        print("\n--- data summary snippet ---")
        df.select(df.columns[:3] + [rate_col]).show(20, truncate=False)
        
        # count total versus flagged pinned edge values
        total_count = df.count()
        suspicious_count = df.filter((F.col(rate_col) == 0.0) | (F.col(rate_col) == 100.0) | (F.col(rate_col) == 1.0)).count()
        
        print(f"total rows analyzed: {total_count}")
        print(f"rows pinned exactly at edge thresholds (0, 1, or 100): {suspicious_count} ({round((suspicious_count/total_count)*100, 2) if total_count > 0 else 0}%)")
        
except Exception as e:
    print(f"execution failed: {e}")
