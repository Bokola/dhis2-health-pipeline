import os
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from utils.schemas import METADATA_SCHEMA, ORG_UNITS_SCHEMA, PROGRAMS_SCHEMA, DATA_VALUES_SCHEMA
from utils.logging_config import get_logger

logger = get_logger("Task01_Ingestion")

def run_task_01(spark: SparkSession, data_dir: str, output_dir: str) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    # load files safely using explicit schemas
    logger.info("ingesting raw structural json datasets")
    
    def get_path(filename: str) -> str:
        p = os.path.join(data_dir, filename)
        if not os.path.exists(p):
            raise FileNotFoundError(f"missing required tracking data targets {p}")
        return p

    raw_meta = spark.read.schema(METADATA_SCHEMA).json(get_path("metadata.json"))
    raw_ou = spark.read.schema(ORG_UNITS_SCHEMA).json(get_path("org_units.json"))
    raw_prog = spark.read.schema(PROGRAMS_SCHEMA).json(get_path("programs.json"))
    raw_dv = spark.read.schema(DATA_VALUES_SCHEMA).json(get_path("data_values.json"))

    df_de = raw_meta.select(F.explode("dataElements").alias("de")).select("de.*")
    df_ou = raw_ou.select(F.explode("organisationUnits").alias("ou")).select("ou.*")
    df_prog = raw_prog.select(F.explode("programs").alias("prog")).select("prog.*")
    df_dv_flat = raw_dv.select(F.explode("dataValues").alias("dv")).select("dv.*")

    # separate and quarantine entries missing primary structural keys
    quarantine_cond = (
        F.col("dataElement").isNull() | 
        F.col("period").isNull() | 
        F.col("orgUnit").isNull()
    )
    df_quarantine = df_dv_flat.filter(quarantine_cond)
    df_dv = df_dv_flat.filter(~quarantine_cond)

    q_count = df_quarantine.count()
    if q_count > 0:
        logger.warning(f"quarantining unresolvable entries to target layout paths")
        quarantine_path = os.path.join(output_dir, "quarantine", "malformed_records.parquet")
        df_quarantine.write.mode("overwrite").parquet(quarantine_path)
    
    return df_dv, df_de, df_ou, df_prog
