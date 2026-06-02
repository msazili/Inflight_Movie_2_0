from statistics import mode

from pyspark.sql import SparkSession
from pathlib import Path
from ..utils import get_load_timestamp


dataset_path = Path("dataset")
bronze_db    = Path("database/bronze")

# Create folders if they don't exist
bronze_db.mkdir(parents=True, exist_ok=True)

def trigger_bronze(spark: SparkSession):

    print("\tStarting Bronze Layer...")
    
    try:
        df_links   = spark.read.format("csv").option("header", True).load(str(dataset_path / "links.csv"))
        df_movies  = spark.read.format("csv").option("header", True).load(str(dataset_path / "movies.csv"))
        df_ratings = spark.read.format("csv").option("header", True).load(str(dataset_path / "ratings.csv"))
        df_tags    = spark.read.format("csv").option("header", True).load(str(dataset_path / "tags.csv"))

        df_links   = df_links.withColumn("load_timestamp", get_load_timestamp())
        df_movies  = df_movies.withColumn("load_timestamp", get_load_timestamp())
        df_ratings = df_ratings.withColumn("load_timestamp", get_load_timestamp())
        df_tags    = df_tags.withColumn("load_timestamp", get_load_timestamp())
        
        df_links.write.format("parquet").mode("overwrite").save(str(bronze_db / "links"))
        df_movies.write.format("parquet").mode("overwrite").save(str(bronze_db / "movies"))
        df_ratings.write.format("parquet").mode("overwrite").save(str(bronze_db / "ratings"))
        df_tags.write.format("parquet").mode("overwrite").save(str(bronze_db / "tags"))        
        
        print(f"\tBronze Layer completed => "
              f"Links: {df_links.count():,}, "
              f"Ratings: {df_ratings.count():,}, "
              f"Movies: {df_movies.count():,}, "
              f"Tags: {df_tags.count():,}\n")
        
    except Exception as e:
        print(f"\tError in Bronze layer: {e}\n")
        raise