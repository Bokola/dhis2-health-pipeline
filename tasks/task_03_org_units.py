import os
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from utils.logging_config import get_logger

logger = get_logger("Task03_OrgUnits")

def run_task_03(df_dv: DataFrame, df_ou: DataFrame, output_dir: str) -> DataFrame:
    # parse tree relationships dynamically without utilizing manual depth constants
    logger.info("resolving geographical regional lineage mappings")
    
    ghost_ou_df = df_dv.join(F.broadcast(df_ou), df_dv["orgUnit"] == df_ou["id"], "left_anti")
    ghost_count = ghost_ou_df.count()
    
    if ghost_count > 0:
        logger.warning(f"orphaned units tracked inside raw execution frames")
        ghost_path = os.path.join(output_dir, "quarantine", "orphaned_org_units.parquet")
        ghost_ou_df.write.mode("overwrite").parquet(ghost_path)
        
    # strip leading and trailing slashes safely using regex before splitting the array
    df_ou_clean = df_ou.withColumn("clean_path", F.regexp_replace(F.col("path"), "^/|/$", ""))
    df_ou_split = df_ou_clean.withColumn("path_arr", F.split(F.col("clean_path"), "/"))
    
    df_fac = df_ou_split.filter(F.col("level") == 4).select(
        F.col("id").alias("facility_id"),
        F.col("name").alias("facility_name"),
        F.col("level").alias("facility_level"),
        F.col("path_arr")[0].alias("l1_id"),
        F.col("path_arr")[1].alias("l2_id"),
        F.col("path_arr")[2].alias("l3_id")
    )
    
    df_levels = df_fac         .join(df_ou.select(F.col("id").alias("id_l1"), F.col("name").alias("country_name")), F.col("l1_id") == F.col("id_l1"), "left")         .join(df_ou.select(F.col("id").alias("id_l2"), F.col("name").alias("region_name")), F.col("l2_id") == F.col("id_l2"), "left")         .join(df_ou.select(F.col("id").alias("id_l3"), F.col("name").alias("district_name")), F.col("l3_id") == F.col("id_l3"), "left")         .select("facility_id", "facility_name", "facility_level", "district_name", "region_name", "country_name")

    final_df = df_dv.join(df_levels, df_dv["orgUnit"] == df_levels["facility_id"], "inner").drop("facility_id")
    return final_df
