import argparse
import sys
from pyspark.sql import SparkSession
from utils.logging_config import get_logger
from tasks.task_01_ingestion import run_task_01
from tasks.task_02_metadata import run_task_02
from tasks.task_03_org_units import run_task_03
from tasks.task_04_quality_flags import run_task_04
from tasks.task_05_dimensional_model import run_task_05
from tasks.task_06_analytics import run_task_06
from tasks.task_07_aggregations import run_task_07

logger = get_logger("Pipeline_Orchestrator")

def main() -> None:
    # coordinate processing layers sequentially across dependencies
    parser = argparse.ArgumentParser(description="orchestrated execution flow engine for dhis2 logs")
    parser.add_argument("--data-dir", type=str, default="./data", help="raw input files repository path")
    parser.add_argument("--output-dir", type=str, default="./output", help="destination directory for processing nodes")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("DHIS2_Health_Data_Pipeline") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()

    try:
        logger.info("starting execution matrix paths")
        
        df_dv, df_de, df_ou, df_prog = run_task_01(spark, args.data_dir, args.output_dir)
        df_resolved = run_task_02(df_dv, df_de, args.output_dir)
        df_spatial = run_task_03(df_resolved, df_ou, args.output_dir)
        
        df_transformed = run_task_04(df_spatial, df_prog)
        df_transformed.cache()

        if df_transformed.count() == 0:
            logger.error("empty dataframe produced across execution nodes")
            sys.exit(1)

        run_task_05(df_transformed, args.output_dir)
        run_task_06(df_transformed, args.output_dir)
        run_task_07(df_transformed, args.output_dir)

        logger.info("processing framework completed successfully")

    except Exception as e:
        logger.error(f"uncaught exception raised across processing elements {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
