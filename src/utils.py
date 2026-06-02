import os 
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


def create_spark_session():

    print("------------------------------------")
    print("## Creating Spark Session...\n")

    spark = (SparkSession.builder
             .appName("Inflight Entertainment Analytics")
             .master("local[*]")
             .config("spark.sql.adaptive.enabled", "true")        # Performance optimization
             .config("spark.sql.shuffle.partitions", "100")       # Control parallelism
             .getOrCreate())

    print("\n## Spark Session created successfully")
    print("------------------------------------\n")

    return spark


def get_load_timestamp():
    return current_timestamp()