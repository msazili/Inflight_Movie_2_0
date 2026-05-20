from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_timestamp, from_unixtime, year, month, regexp_extract, trim, when
from pathlib import Path

bronze_db = Path("database/bronze")
silver_db = Path("database/silver")

def trigger_silver(spark: SparkSession):

    print("\tStarting Silver Layer...")
    
    try:
        # Read from Bronze Delta
        links = spark.read.parquet(str(bronze_db / "links"))
        movies = spark.read.parquet(str(bronze_db / "movies"))
        ratings = spark.read.parquet(str(bronze_db / "ratings"))
        tags = spark.read.parquet(str(bronze_db / "tags"))

        links_clean = (
            links
            .withColumn("movieId", col("movieId").cast("int"))
            .withColumn("imdbId", col("imdbId").cast("int"))
            .withColumn("tmdbId", col("tmdbId").cast("int"))
        )

        tags_clean = (
            tags
            .withColumn("userId", col("userId").cast("int"))
            .withColumn("movieId", col("movieId").cast("int"))
            .withColumn("datetime", from_unixtime(col("timestamp").cast("long")))
            .withColumn("tags_year", year(col("datetime")))
            .withColumn("tags_month", month(col("datetime")))
        )

        ratings_clean = (
            ratings
            .orderBy("timestamp")
            .dropDuplicates(["userId", "movieId"])
            #.withColumn("datetime", to_timestamp(col("timestamp")))
            .withColumn("datetime", from_unixtime(col("timestamp").cast("long")))
            .withColumn("rating_year", year(col("datetime")))
            .withColumn("rating_month", month(col("datetime")))
            .withColumn("userId", col("userId").cast("int"))
            .withColumn("movieId", col("movieId").cast("int"))
            .withColumn("rating", col("rating").cast("float"))
        )

        # Regex pattern for checking and extracting the year at the end
        year_pattern = r"\((\d{4})\)$"
        full_pattern = r"(.*?)\s*\((\d{4})\)$"

        movies_clean = (
            movies
            .withColumn("movieId", col("movieId").cast("int"))
            .withColumn("movieTitle", when(col("title").rlike(year_pattern), trim(regexp_extract("title", full_pattern, 1))).otherwise(trim(col("title"))))
            .withColumn("year", when(col("title").rlike(year_pattern), regexp_extract("title", full_pattern, 2)).otherwise(None).cast("int"))
            .withColumn("genres_list", split(col("genres"), "\\|"))
        )

        links_clean.write.mode("overwrite").parquet(str(silver_db / "links"))
        movies_clean.write.mode("overwrite").parquet(str(silver_db / "movies"))
        ratings_clean.write.mode("overwrite").parquet(str(silver_db / "ratings"))
        tags_clean.write.mode("overwrite").parquet(str(silver_db / "tags"))

        print(f"\tSilver Layer completed => "
              f"Links: {links_clean.count():,}, "
              f"Ratings: {ratings_clean.count():,}, "
              f"Movies: {movies_clean.count():,}, "
              f"Tags: {tags_clean.count():,}\n")
        
    except Exception as e:
        print(f"Error in Silver layer: {e}\n")
        raise