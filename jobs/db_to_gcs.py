from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def db_to_gcs(spark: SparkSession, 
              url_env: str,
              username_env: str,
              driver: str,
              password_env: str,
              table_name: str, 
              gcs_path: str,
              read_partition_column: Optional[str],
              write_partition_value: Optional[str],
              lower_bound: Optional[int],
              upper_bound: Optional[int]) -> None:
    logger.info(f"Starting DB to GCS job for table: {table_name}")
    
    jdbc_url = os.getenv(url_env)
    user = os.getenv(username_env)
    password = os.getenv(password_env)

    if not all([jdbc_url, user, password]):
        logger.info(f"Environment variables - url_env: {jdbc_url}, username_env: {user}, password_env: {'****' if password else None}")
        raise ValueError("Missing DB environment variables")
    
    properties = {
        "user": os.getenv(username_env),
        "password": os.getenv(password_env),
        "driver": driver,
        "fetchsize": "5000"
    }
    
    try:
        if read_partition_column:
            df = spark.read.jdbc(
                url=jdbc_url,
                table=table_name,
                column=read_partition_column,
                lowerBound=lower_bound,
                upperBound=upper_bound,
                numPartitions=10,
                properties=properties
            )
        else:
            df = spark.read.jdbc(
                url=jdbc_url,
                table=table_name,
                properties=properties
            )
        
        bronze_df = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_table", lit(table_name))
            
        if write_partition_value:
            bronze_df = bronze_df \
                .withColumn("date", to_date(col(lit(write_partition_value)))) \
                .withColumn("year", year(col("date"))) \
                .withColumn("month", month(col("date"))) \
                .withColumn("day", dayofmonth(col("date"))) 
            
            bronze_df.write.partitionBy("year", "month", "day").mode("overwrite").parquet(gcs_path)
        else:
            bronze_df.write.mode("overwrite").parquet(gcs_path)
            
        logger.info(f"Data from {table_name} successfully written to {gcs_path}")
    except Exception as e:
        logger.error(f"Error processing table {table_name}: {e}", exc_info= True)
        raise

def main():
    spark = SparkSession.builder.appName("DBToGCS").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    if len(sys.argv) < 4:
        raise ValueError("Missing arguments: 'target_table', 'db_type', and 'data_interval_start' are required")
    
    target_table = sys.argv[1]
    db_type = sys.argv[2]
    data_interval_start = sys.argv[3]

    if db_type == "sqlserver":
        url_env = "SQLSERVER_URL"
        username_env = "SQLSERVER_USERNAME"
        password_env = "SQLSERVER_PASSWORD"
        driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    elif db_type == "postgres":
        url_env = "POSTGRES_URL"
        username_env = "POSTGRES_USERNAME"
        password_env = "POSTGRES_PASSWORD"
        driver = "org.postgresql.Driver"
    else:
        raise ValueError("Invalid 'db_type' argument")

    db_to_gcs(
        spark=spark,
        url_env=url_env,
        username_env=username_env,
        password_env=password_env,
        driver=driver,
        table_name=target_table,
        gcs_path=f"gs://sotatek-k8s-prac-bronze/{target_table}",
        read_partition_column="id",
        write_partition_value=data_interval_start,
        lower_bound=1,
        upper_bound=5000000
    )
    
    spark.stop()
    
if __name__ == "__main__":
    main()