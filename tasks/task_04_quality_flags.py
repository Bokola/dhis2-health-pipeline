from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, last_day, to_date, concat_ws, substring, datediff, row_number, when, expr, lit
from pyspark.sql.window import Window

def run_task_04(df_spatial: DataFrame, df_prog_raw: DataFrame) -> DataFrame:
    # process programs schema and drop country to avoid join collisions
    df_prog = df_prog_raw.select(
        col("id").alias("program_id"),
        col("healthArea"),
        col("country").alias("prog_country"),
        explode(col("dataElements")).alias("de_id")
    ).distinct()

    # maintain all structural elements and metadata properties required downstream
    df_base = df_spatial.select(
        col("country_name"),
        col("facility_name"),
        col("facility_level"),
        col("district_name"),
        col("region_name"),
        col("dataElement"),
        col("name"),
        col("valueType"),
        col("period"),
        col("orgUnit"),
        col("categoryOptionCombo"),
        col("attributeOptionCombo"),
        col("value"),
        col("storedBy"),
        col("created"),
        col("lastUpdated"),
        col("followup")
    ).filter(
        col("dataElement").isNotNull() & 
        col("period").isNotNull() & 
        col("orgUnit").isNotNull()
    )

    # step by step casting rules and unified type resolution
    df_casted = df_base \
        .withColumn("p_year", substring(col("period"), 1, 4).cast("int")) \
        .withColumn("p_month", substring(col("period"), 5, 2).cast("int")) \
        .withColumn("period_end_date", last_day(to_date(concat_ws("-", col("p_year"), col("p_month"), expr("1")), "yyyy-M-d"))) \
        .withColumn("value_int", col("value").cast("int")) \
        .withColumn("value_double", col("value").cast("double")) \
        .withColumn("value_bool", when(col("value") == "true", True).when(col("value") == "false", False).otherwise(None)) \
        .withColumn(
            "cast_value",
            when(col("valueType").isin("NUMBER", "INTEGER", "INT"), col("value").cast("double"))
            .when(col("valueType") == "BOOLEAN", when(col("value") == "true", 1.0).otherwise(0.0))
            .otherwise(lit(None).cast("double"))
        ) \
        .withColumn("is_late_reported", datediff(to_date(substring(col("lastUpdated"), 1, 10), "yyyy-MM-dd"), col("period_end_date")) > 60) \
        .withColumn("is_explicit_zero", col("value") == "0") \
        .withColumn("is_missing_value", col("value").isNull()) \
        .withColumn(
            "completeness_score",
            when(col("value").isNotNull(), 1.0).otherwise(0.0)
        ) \
        .withColumn(
            "reported_indicators_count",
            when(col("value").isNotNull() & (col("value") != ""), 1).otherwise(0)
        )

    # deduplicate tracking target frames via window evaluation
    window_spec = Window.partitionBy("dataElement", "period", "orgUnit", "categoryOptionCombo").orderBy(col("lastUpdated").desc())
    df_dedup = df_casted.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).drop("rn")

    # execute structural inner merge operation without column ambiguity
    df_transformed = df_dedup.join(
        df_prog,
        (df_dedup.country_name == df_prog.prog_country) & (df_dedup.dataElement == df_prog.de_id),
        "inner"
    ).drop("prog_country")

    return df_transformed
