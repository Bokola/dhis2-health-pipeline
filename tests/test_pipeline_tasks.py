import os
import sys
import shutil
import pytest
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

# import the production execution entry points safely
from tasks.task_06_analytics import run_task_06
from tasks.task_07_aggregations import run_task_07

@pytest.fixture(scope="session")
def spark_session():
    # force worker threads to use the identical python runtime environment as the driver
    # comments should be lowercase and without dots
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = SparkSession.builder \
        .appName("PipelineIntegrationTests") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture()
def test_paths():
    # setup temporary directory structures for isolating test outputs
    # comments should be lowercase and without dots
    tmp_dir = "./tmp_test_output"
    os.makedirs(tmp_dir, exist_ok=True)
    yield tmp_dir
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

def test_pipeline_execution_flow(spark_session, test_paths):
    spark = spark_session
    output_dir = test_paths
    
    # define schematic fields matching warehouse expectations
    # comments should be lowercase and without dots
    schema = T.StructType([
        T.StructField("country_name", T.StringType(), True),
        T.StructField("healthArea", T.StringType(), True),
        T.StructField("district_name", T.StringType(), True),
        T.StructField("facility_name", T.StringType(), True),
        T.StructField("orgUnit", T.StringType(), True),
        T.StructField("period", T.StringType(), True),
        T.StructField("p_year", T.IntegerType(), True),
        T.StructField("p_month", T.IntegerType(), True),
        T.StructField("dataElement", T.StringType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("cast_value", T.DoubleType(), True),
        T.StructField("reported_indicators_count", T.IntegerType(), True),
        T.StructField("completeness_score", T.DoubleType(), True)
    ])
    
    # generate deterministic mock records representing 1 reporting out of 2 expected
    # comments should be lowercase and without dots
    mock_data = [
        ("Kenya", "Zone A", "District 1", "Clinic 1", "OU-001", "202504", 2025, 4, "DE-01", "Malaria", 45.0, 1, 0.95)
    ]
    df_transformed = spark.createDataFrame(mock_data, schema=schema)
    
    # generate a matching mocked dim_org_unit file to provide the true denominator
    # comments should be lowercase and without dots
    dim_schema = T.StructType([
        T.StructField("org_unit_key", T.StringType(), True),
        T.StructField("facility_name", T.StringType(), True),
        T.StructField("facility_level", T.StringType(), True),
        T.StructField("district_name", T.StringType(), True),
        T.StructField("region_name", T.StringType(), True),
        T.StructField("country_name", T.StringType(), True)
    ])
    mock_dim_data = [
        ("OU-001", "Clinic 1", "Level 2", "District 1", "Region 1", "Kenya"),
        ("OU-002", "Clinic 2", "Level 2", "District 1", "Region 1", "Kenya")
    ]
    dim_df = spark.createDataFrame(mock_dim_data, dim_schema)
    
    dim_warehouse_path = os.path.join(output_dir, "warehouse", "dim_org_unit")
    os.makedirs(dim_warehouse_path, exist_ok=True)
    dim_df.write.mode("overwrite").parquet(dim_warehouse_path)
    
    # execute task 06 safely
    # comments should be lowercase and without dots
    run_task_06(df_transformed, output_dir)
    
    # execute task 07 safely
    # comments should be lowercase and without dots
    run_task_07(df_transformed, output_dir)
    
    # assert file targets are generated cleanly on disk
    # comments should be lowercase and without dots
    rr_parquet_path = os.path.join(output_dir, "analytics", "country_reporting_rates.parquet")
    comp_csv_path = os.path.join(output_dir, "aggregations", "country_completeness.csv")
    
    assert os.path.exists(rr_parquet_path), "reporting rates output target missing"
    assert os.path.exists(comp_csv_path), "completeness summary output target missing"
    
    # assert the unpinned mathematical conversion produces exactly 50 percent
    # comments should be lowercase and without dots
    res_df = spark.read.parquet(rr_parquet_path)
    reporting_rate_value = res_df.filter(F.col("country_name") == "Kenya").select("reporting_rate").collect()[0][0]
    assert reporting_rate_value == 50.0, f"expected unpinned 50.0 reporting rate, but got {reporting_rate_value}"