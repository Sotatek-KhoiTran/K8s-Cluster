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
              partition_column: Optional[str],
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
        "fetchsize": "1000"
    }
    
    try:
        if partition_column:
            df = spark.read.jdbc(
                url=jdbc_url,
                table=table_name,
                column=partition_column,
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
            .withColumn("ingestion_date", to_date(col("ingestion_timestamp"))) \
            .withColumn("source_table", lit(table_name))
            
        bronze_df.write.partitionBy("ingestion_date").mode("overwrite").parquet(gcs_path)
        logger.info(f"Data from {table_name} successfully written to {gcs_path}")
    except Exception as e:
        logger.error(f"Error processing table {table_name}: {e}", exc_info= True)
        raise

def main():
    spark = SparkSession.builder.appName("DBToGCS").getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    db_to_gcs(
        spark=spark,
        url_env="SQLSERVER_URL",
        username_env="SQLSERVER_USERNAME",
        password_env="SQLSERVER_PASSWORD",
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
        table_name="orders",
        gcs_path="gs://sotatek-k8s-prac-bronze/orders",
        partition_column="order_id",
        lower_bound=1,
        upper_bound=5000000
    )
    
    db_to_gcs(
        spark=spark,
        url_env="POSTGRES_URL",
        username_env="POSTGRES_USERNAME",
        password_env="POSTGRES_PASSWORD",
        driver="org.postgresql.Driver",
        table_name="customer",
        gcs_path="gs://sotatek-k8s-prac-bronze/customer",
        partition_column="id",
        lower_bound=1,
        upper_bound=5000000
    )
    
    spark.stop()
    
if __name__ == "__main__":
    main()