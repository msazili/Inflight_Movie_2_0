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
             .getOrCreate())

    print("\n## Spark Session created successfully")
    print("------------------------------------\n")

    return spark


def get_load_timestamp():
    return current_timestamp()