from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, count, mean, stddev, col, max as spark_max
from pathlib import Path

silver_db = Path("database/silver")
gold_db = Path("database/gold")

def trigger_gold(spark: SparkSession):

    print("\tStarting Gold Layer...")
    
    try:
        ratings = spark.read.parquet(str(silver_db / "ratings"))
        movies = spark.read.parquet(str(silver_db / "movies"))
        tags = spark.read.parquet(str(silver_db / "tags"))
        
        movie_stats = (ratings.groupBy("movieId")
                       .agg(
                           mean("rating").alias("avg_rating"),
                           count("*").alias("rating_count"),
                           stddev("rating").alias("rating_std")
                       ))

        tag_count = (tags.groupBy("movieId")
                    .agg(count("*").alias("tag_count")))
        
        movies_exploded = movies.withColumn("genre", explode(col("genres_list")))

        movie_genre_gold = (ratings
            .join(movies_exploded.select("movieId", "movieTitle", "genre"), "movieId")
            .join(movie_stats, "movieId")
            .join(tag_count, "movieId", "left")
            .fillna(0, subset=["tag_count"])
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

        movie_gold = (movie_genre_gold
                  .groupBy("movieId", "movieTitle")
                  .agg(
                      mean("avg_rating").alias("avg_rating"),
                      spark_max("rating_count").alias("rating_count"),
                      spark_max("tag_count").alias("tag_count"),
                      mean("rating_std").alias("rating_std")
                  ))

        movie_gold.write.mode("overwrite").parquet(str(gold_db / "movies_gold"))
        movie_genre_gold.write.mode("overwrite").parquet(str(gold_db / "movie_genre_gold"))

        print("\tGold Layer completed successfully\n")

        return movie_gold, movie_genre_gold
    
    except Exception as e:
        print(f"\tError in Gold layer: {e}\n")
        raise