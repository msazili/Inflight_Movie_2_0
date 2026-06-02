from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, count, mean, stddev, col, max
from pathlib import Path

silver_db = Path("database/silver")
gold_db = Path("database/gold")

# Create folders if they don't exist
silver_db.mkdir(parents=True, exist_ok=True)
gold_db.mkdir(parents=True, exist_ok=True)

def trigger_gold(spark: SparkSession):

    print("\tStarting Gold Layer...")
    
    try:
        df_ratings = spark.read.format("parquet").load(str(silver_db / "ratings"))
        df_movies = spark.read.format("parquet").load(str(silver_db / "movies"))
        df_tags = spark.read.format("parquet").load(str(silver_db / "tags"))
        
        df_movie_stats = (df_ratings.groupBy("movieId")
                       .agg(
                           mean("rating").alias("avg_rating"),
                           count("*").alias("rating_count"),
                           stddev("rating").alias("rating_std")
                       ))

        df_tag_count = (df_tags.groupBy("movieId")
                    .agg(count("*").alias("tag_count")))
        
        df_movies_exploded = df_movies.withColumn("genre", explode(col("genres_list")))


        # Prepare movies data (only needed columns)
        df_movies_clean = df_movies_exploded.select("movieId", "movieTitle", "genre")

        # Join ratings with movie information
        df_with_movies = df_ratings.join(df_movies_clean, "movieId")

        # Add movie statistics (avg_rating, rating_count, rating_std)
        df_with_stats = df_with_movies.join(df_movie_stats, "movieId")

        # Add tag counts (left join to keep movies without tags)
        df_with_tags = df_with_stats.join(df_tag_count, "movieId", "left")

        # Replace null tag counts with 0
        df_with_tags_filled = df_with_tags.fillna(0, subset=["tag_count"])

        df_movie_genre_gold = (df_with_tags_filled
            .select(
                "movieId",
                "movieTitle",
                "genre",
                "rating",
                "rating_year",
                "rating_month",
                "avg_rating",
                "rating_count",
                "tag_count",
                "rating_std"
            )
            .orderBy(col("rating_count").desc(), col("avg_rating").desc())
        )

        df_movie_gold = (df_movie_genre_gold
                  .groupBy("movieId", "movieTitle")
                  .agg(
                      mean("avg_rating").alias("avg_rating"),
                      max("rating_count").alias("rating_count"),
                      max("tag_count").alias("tag_count"),
                      mean("rating_std").alias("rating_std")
                  ))

        # Save to parquet file
        df_movie_gold.write.format("parquet").mode("overwrite").save(str(gold_db / "movies_gold"))
        df_movie_genre_gold.write.format("parquet").mode("overwrite").save(str(gold_db / "movie_genre_gold"))

        print("\tGold Layer completed successfully\n")

    except Exception as e:
        print(f"\tError in Gold layer: {e}\n")
        raise