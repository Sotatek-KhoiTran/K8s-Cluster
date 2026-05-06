from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from datetime import datetime, timedelta

import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_dq_checks(spark: SparkSession, table_name: str, gcs_path: str, data_interval_start: str):
    year = datetime.strptime(data_interval_start, "%Y-%m-%d").year
    month = datetime.strptime(data_interval_start, "%Y-%m-%d").month
    day = datetime.strptime(data_interval_start, "%Y-%m-%d").day
    path = f"{gcs_path}/year={year}/month={month}/day={day}"
    
    df = spark.read.parquet(path)
    
    errors = []
    
    # Volume Check
    record_count = df.count()
    if record_count == 0:
        errors.append("Volume Check Failed - Table is empty .")
    else:
        logger.info(f"Volume Check Passed: {record_count} records found.")
        
    # Freshness Check
    if "created_at" in df.columns:
        max_date_row = df.agg(max("created_at").alias("max_date")).collect()[0]
        max_date = max_date_row["max_date"]
        
        if max_date is None:
            max_date = "1970-01-01"
        if max_date < datetime.now() - timedelta(days=1):
            errors.append(f"Freshness Check Failed - Most recent record is on: {max_date}.")
        else:
            logger.info(f"Freshness Check Passed: Most recent record is on: {max_date}.")
    else:
        logger.info("Freshness Check Skipped: 'created_at' column not found.")
    
    # Uniqueness Check
    distinct_ids = df.select("id").distinct().count()
    if distinct_ids != record_count:
        errors.append(f"Uniqueness Check Failed - Expected {distinct_ids} distinct IDs, but found {record_count}.")
    else:
        logger.info(f"Uniqueness Check Passed: {distinct_ids} distinct IDs found.")

    # Table specific checks
    if table_name == "orders":
        check_cols = ["id", "user_id", "created_at"]
        df_null = df.select([
            F.sum(F.col(c).isNull().cast("int")).alias(c)
            for c in check_cols
        ])
        null_counts = df_null.collect()[0].asDict()
        for col, null_count in null_counts.items():
            if null_count > 0:
                errors.append(f"Null Check Failed - Column '{col}' has {null_count} null values.")
            else:
                logger.info(f"Null Check Passed: Column '{col}' has no null values.")
    
    if table_name == "customer":
        check_cols = ["id", "user_name", "age", "created_at"]
        df_null = df.select([
            F.sum(F.col(c).isNull().cast("int")).alias(c)
            for c in check_cols
        ])
        null_counts = df_null.collect()[0].asDict()
        for col, null_count in null_counts.items():
            if null_count > 0:
                errors.append(f"Null Check Failed - Column '{col}' has {null_count} null values.")
            else:
                logger.info(f"Null Check Passed: Column '{col}' has no null values.")
                
        invalid_age_count = df.filter((F.col("age").cast("int") < 18) | (F.col("age").cast("int") > 100)).count()
        if invalid_age_count > 0:   
            errors.append(f"Numeric Distribution Failed: Found {invalid_age_count} records with invalid ages.")
            
    if errors:
        logger.error(f"Data Quality Checks Failed for table '{table_name}':\n")
        for error in errors:
            logger.error(f"- {error}")
        raise Exception(f"Data Quality checks failed for {table_name}. See logs for details.")
    else:
        logger.info(f"All Data Quality Checks Passed for table '{table_name}'.")
        
if __name__ == "__main__":
    spark = SparkSession.builder.appName("DB Ingestion DQ Checks").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    if len(sys.argv) < 4:
        raise ValueError("Missing arguments: 'table_name', 'gcs_path' and 'data_interval_start' are required")
    
    table_name = sys.argv[1]
    gcs_path = sys.argv[2]
    data_interval_start = sys.argv[3]

    run_dq_checks(spark, table_name, gcs_path, data_interval_start)
    
    spark.stop()