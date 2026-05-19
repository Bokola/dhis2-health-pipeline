import os
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from utils.logging_config import get_logger

logger = get_logger("Task06_Analytics")

def run_task_06(df: DataFrame, output_dir: str) -> None:
    # track window variations and find underreporting tracks
    logger.info("generating window analytics tracking profiles")
    analytics_path = os.path.join(output_dir, "analytics")
    
    w_mom = Window.partitionBy("dataElement", "district_name").orderBy("period")
    df_mom = df.groupBy("dataElement", "name", "district_name", "period") \
               .agg(F.sum("cast_value").alias("total_value")) \
               .withColumn("prev_value", F.lag("total_value").over(w_mom)) \
               .withColumn("mom_pct_change", ((F.col("total_value") - F.col("prev_value")) / F.col("prev_value")) * 100)
    df_mom.write.mode("overwrite").parquet(os.path.join(analytics_path, "mom_indicators.parquet"))
    
    w_roll = Window.partitionBy("orgUnit", "dataElement").orderBy("period").rowsBetween(-2, 0)
    df_roll = df.groupBy("orgUnit", "facility_name", "dataElement", "name", "period") \
                .agg(F.avg("cast_value").alias("monthly_avg")) \
                .withColumn("three_month_rolling_avg", F.avg("monthly_avg").over(w_roll))
    df_roll.write.mode("overwrite").parquet(os.path.join(analytics_path, "rolling_averages.parquet"))
    
 # retrieve spark session from dataframe
    spark = df.sparkSession
    
    # load the complete dimensions registry to find total expected facilities
    dim_org_df = spark.read.parquet(os.path.join(output_dir, "warehouse", "dim_org_unit"))
    
    # count total expected facilities per country
    # comments should be lowercase and without dots
    expected_facilities = dim_org_df \
        .groupBy("country_name") \
        .agg(F.count_distinct("org_unit_key").alias("total_expected_facilities"))
        
    # count actual facilities that reported from the facts layer
    # comments should be lowercase and without dots
    actual_reporting = df.groupBy("country_name", "period").agg(
        F.count_distinct(F.when(F.col("reported_indicators_count") > 0, F.col("orgUnit"))).alias("total_actual_reporting")
    )
    
    # join layers to compute genuine reporting rates safely
    df_rr = actual_reporting \
        .join(expected_facilities, on="country_name", how="inner") \
        .withColumn("reporting_rate", F.round((F.col("total_actual_reporting") / F.col("total_expected_facilities")) * 100, 2)) \
        .select("country_name", "period", "reporting_rate")
        
    df_rr.write.mode("overwrite").parquet(os.path.join(analytics_path, "country_reporting_rates.parquet"))
    
    df_zero_periods = df.groupBy("healthArea", "orgUnit", "facility_name", "period") \
                        .agg(F.sum(F.when(F.col("cast_value") > 0, 1).otherwise(0)).alias("positive_counts")) \
                        .filter(F.col("positive_counts") == 0) \
                        .groupBy("healthArea", "orgUnit", "facility_name") \
                        .agg(F.count("period").alias("periods_with_zero_data"))
                        
    w_rank = Window.partitionBy("healthArea").orderBy(F.col("periods_with_zero_data").desc())
    df_underreporting = df_zero_periods.withColumn("rank", F.rank().over(w_rank)).filter(F.col("rank") <= 5)
    df_underreporting.write.mode("overwrite").parquet(os.path.join(analytics_path, "top_underreporting_facilities.parquet"))