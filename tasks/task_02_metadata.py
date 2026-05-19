import os
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from utils.logging_config import get_logger

logger = get_logger("Task02_Metadata")

def run_task_02(df_dv: DataFrame, df_de: DataFrame, output_dir: str) -> DataFrame:
    # check matching identifiers using optimized broadcast transformations
    logger.info("resolving metadata identities across records")
    
    ghost_de_df = df_dv.join(F.broadcast(df_de), df_dv["dataElement"] == df_de["id"], "left_anti")
    ghost_count = ghost_de_df.count()
    
    if ghost_count > 0:
        logger.warning(f"isolated unmapped metrics identifiers inside staging tracks")
        ghost_path = os.path.join(output_dir, "quarantine", "unresolvable_metadata.parquet")
        ghost_de_df.write.mode("overwrite").parquet(ghost_path)
        
    resolved_df = df_dv.join(
        F.broadcast(df_de.select("id", "name", "valueType")),
        df_dv["dataElement"] == df_de["id"],
        "inner"
    ).drop("id")
    
    return resolved_df
