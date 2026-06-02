from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_timestamp, from_unixtime, year, month, regexp_extract, trim, when
from pathlib import Path

bronze_db = Path("database/bronze")
silver_db = Path("database/silver")

# Create folders if they don't exist
bronze_db.mkdir(parents=True, exist_ok=True)
silver_db.mkdir(parents=True, exist_ok=True)

def trigger_silver(spark: SparkSession):

    print("\tStarting Silver Layer...")
    
    try:
        # Read from Bronze Delta
        df_links = spark.read.format("parquet").load(str(bronze_db / "links"))

        df_links_clean = (
            df_links
            .withColumn("movieId", col("movieId").cast("int"))
            .withColumn("imdbId", col("imdbId").cast("int"))
            .withColumn("tmdbId", col("tmdbId").cast("int"))
        )


        df_movies = spark.read.format("parquet").load(str(bronze_db / "movies"))

        # Regex pattern for checking and extracting the year at the end
        year_pattern = r"\((\d{4})\)$"
        full_pattern = r"(.*?)\s*\((\d{4})\)$"

        df_movies_clean = (
            df_movies
            .withColumn("movieId", col("movieId").cast("int"))
            .withColumn("title", trim(col("title")))
            .withColumn("movieTitle", 
                        when(
                            col("title").rlike(year_pattern), trim(regexp_extract("title", full_pattern, 1))
                        ).otherwise(trim(col("title"))))
            .withColumn("year", 
                        when(
                            col("title").rlike(year_pattern), regexp_extract("title", full_pattern, 2)
                        ).otherwise(None).cast("int"))
            .withColumn("genres_list", split(col("genres"), "\\|"))
        )


        df_ratings = spark.read.format("parquet").load(str(bronze_db / "ratings"))

        df_ratings_clean = (
            df_ratings
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


        df_tags = spark.read.format("parquet").load(str(bronze_db / "tags"))

        df_tags_clean = (
            df_tags
            .withColumn("userId", col("userId").cast("int"))
            .withColumn("movieId", col("movieId").cast("int"))
            .withColumn("datetime", from_unixtime(col("timestamp").cast("long")))
            .withColumn("tags_year", year(col("datetime")))
            .withColumn("tags_month", month(col("datetime")))
        )


        # Write cleaned data to Silver Delta
        df_links_clean.write.format("parquet").mode("overwrite").save(str(silver_db / "links"))
        df_movies_clean.write.format("parquet").mode("overwrite").save(str(silver_db / "movies"))
        df_ratings_clean.write.format("parquet").mode("overwrite").save(str(silver_db / "ratings"))
        df_tags_clean.write.format("parquet").mode("overwrite").save(str(silver_db / "tags"))

        print(f"\tSilver Layer completed => "
              f"Links: {df_links_clean.count():,}, "
              f"Ratings: {df_ratings_clean.count():,}, "
              f"Movies: {df_movies_clean.count():,}, "
              f"Tags: {df_tags_clean.count():,}\n")
        
    except Exception as e:
        print(f"Error in Silver layer: {e}\n")
        raise