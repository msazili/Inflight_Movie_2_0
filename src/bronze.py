from pyspark.sql import SparkSession
from pathlib import Path
from .utils import get_load_timestamp


dataset_path = Path("dataset")
bronze_db    = Path("database/bronze")

def trigger_bronze(spark: SparkSession):

    print("\tStarting Bronze Layer...")
    
    try:
        links   = spark.read.option("header", "true").csv(str(dataset_path / "links.csv"))
        movies  = spark.read.option("header", "true").csv(str(dataset_path / "movies.csv"))
        ratings = spark.read.option("header", "true").csv(str(dataset_path / "ratings.csv"))
        tags    = spark.read.option("header", "true").csv(str(dataset_path / "tags.csv"))

        links   = links.withColumn("load_timestamp", get_load_timestamp())
        movies  = movies.withColumn("load_timestamp", get_load_timestamp())
        ratings = ratings.withColumn("load_timestamp", get_load_timestamp())
        tags    = tags.withColumn("load_timestamp", get_load_timestamp())
        
        links.write.mode("overwrite").parquet(str(bronze_db / "links"))
        movies.write.mode("overwrite").parquet(str(bronze_db / "movies"))
        ratings.write.mode("overwrite").parquet(str(bronze_db / "ratings"))
        tags.write.mode("overwrite").parquet(str(bronze_db / "tags"))        
        
        print(f"\tBronze Layer completed => "
              f"Links: {links.count():,}, "
              f"Ratings: {ratings.count():,}, "
              f"Movies: {movies.count():,}, "
              f"Tags: {tags.count():,}\n")
        
    except Exception as e:
        print(f"\tError in Bronze layer: {e}\n")
        raise