import os
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from utils.logging_config import get_logger

logger = get_logger("Task07_Aggregations")

def run_task_07(df: DataFrame, output_dir: str) -> None:
    # compute summarized country comparisons and write out
    logger.info("exporting data matrix summaries to files")
    agg_path = os.path.join(output_dir, "aggregations")
    
    df_q = df.withColumn("quarter", F.concat(F.col("p_year"), F.lit("-Q"), F.ceil(F.col("p_month") / 3)))
    
    df_volumes = df_q.groupBy("healthArea", "quarter").agg(F.sum("cast_value").alias("total_service_volume"))
    df_volumes.write.mode("overwrite").csv(os.path.join(agg_path, "global_volumes.csv"), header=True)
    
    df_comp = df.groupBy("country_name").agg(F.avg("completeness_score").alias("avg_completeness"))
    df_comp.write.mode("overwrite").csv(os.path.join(agg_path, "country_completeness.csv"), header=True)
    
    df_matrix = df.groupBy("name").pivot("country_name").agg(F.count_distinct("orgUnit"))
    df_matrix.write.mode("overwrite").csv(os.path.join(agg_path, "data_element_coverage_matrix.csv"), header=True)
