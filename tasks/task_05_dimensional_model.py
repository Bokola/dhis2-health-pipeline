import os
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from utils.logging_config import get_logger

logger = get_logger("Task05_DimensionalModel")

def run_task_05(df_transformed: DataFrame, output_dir: str) -> None:
    # export records partitioned cleanly into database structures
    logger.info("building analytical warehouse relational schemas")
    
    dim_de = df_transformed.select(
        F.col("dataElement").alias("data_element_key"),
        F.col("name").alias("data_element_name"),
        "valueType"
    ).distinct()
    
    dim_ou = df_transformed.select(
        F.col("orgUnit").alias("org_unit_key"),
        "facility_name", "facility_level", "district_name", "region_name", "country_name"
    ).distinct()
    
    dim_period = df_transformed.select(
        F.col("period").alias("period_key"),
        F.col("p_year").alias("year"),
        F.col("p_month").alias("month")
    ).distinct()
    
    dim_program = df_transformed.select(
        F.col("program_id").alias("program_key"),
        "healthArea"
    ).distinct()
    
    fact_service = df_transformed.select(
        F.col("dataElement").alias("data_element_key"),
        F.col("period").alias("period_key"),
        F.col("orgUnit").alias("org_unit_key"),
        F.col("program_id").alias("program_key"),
        "categoryOptionCombo", "attributeOptionCombo", "storedBy", "created", "lastUpdated",
        "cast_value", "is_late_reported", "is_explicit_zero", "is_missing_value", "completeness_score",
        "healthArea", F.col("period").alias("year_month")
    )
    
    def write_parquet(df: DataFrame, name: str, partition_cols: list[str] = None) -> None:
        p = os.path.join(output_dir, "warehouse", name)
        writer = df.write.mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.parquet(p)
        logger.info(f"saved star schema entity configuration {name}")

    write_parquet(dim_de, "dim_data_element")
    write_parquet(dim_ou, "dim_org_unit")
    write_parquet(dim_period, "dim_period")
    write_parquet(dim_program, "dim_program")
    write_parquet(fact_service, "fact_service_delivery", partition_cols=["healthArea", "year_month"])
