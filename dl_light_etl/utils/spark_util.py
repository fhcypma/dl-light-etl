import logging
from typing import Dict
from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str, log_level: str = "WARN", config: Dict[str, str] = None
) -> SparkSession:
    """Create spark session for local or Lambda use"""
    logging.info(f"Spark config: {config}")
    spark_builder = SparkSession.builder.appName(app_name)
    [spark_builder := spark_builder.config(k, v) for k, v in config.items()]

    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark


def get_spark() -> SparkSession:
    """Get already running spark session"""
    return SparkSession.builder.getOrCreate()
