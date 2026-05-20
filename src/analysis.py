from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, lit, log1p, explode, mean, round
from pathlib import Path


gold_db = Path("database/gold")

charts_dir = Path("output/charts")
catalog_dir = Path("output/catalog")


def analyze_top_genres(spark: SparkSession):

    # What are the top 10 genres by total number of ratings, 
    # and how has their popularity evolved over time (by year of rating)?

    try:
        # Read from Gold Layer
        movie_genre_gold = spark.read.parquet(str(gold_db / "movie_genre_gold"))
        

        # Top 10 Genres by Total Ratings
        grand_total = movie_genre_gold.count()

        top_10_genres = (movie_genre_gold
            .groupBy("genre")
            .agg(count("*").alias("total_ratings"))
            .withColumn("percentage", round((col("total_ratings") / lit(grand_total) * 100), 2))
            .orderBy(col("total_ratings").desc())
            .limit(10))

        print("=== Top 10 Genres by Total Number of Ratings ===")

        top_10_genres.show(truncate=False)


        # Popularity Evolution Over Time (Yearly Trend)
        yearly_trend = (
            movie_genre_gold
                .groupBy("rating_year", "genre")
                .agg(count("*").alias("total_ratings"))
                .orderBy("rating_year", col("total_ratings").desc())
        )

        # Filter to top 10 genres only
        top_genres_list = [row['genre'] for row in top_10_genres.select("genre").collect()]
        
        top_genres_trend = yearly_trend.filter(col("genre").isin(top_genres_list))

        print("=== Genres Popularity over time by rating year ===")
    
        (
        top_genres_trend
            .filter(col("rating_year") >= 1996)
            .orderBy("rating_year", col("total_ratings").desc())
            .show(10, truncate=False)
        )

        return top_10_genres, top_genres_trend

    except Exception as e:
        print(f"Error in genre analysis: {e}")
        raise    


def find_hidden_gems(spark: SparkSession):

    # Identify "hidden gems": movies with fewer than 50 ratings but an average rating above 4.0. 
    # Which genres are over-represented in this segment compared to the overall genre distribution?

    try:
        # Read from Gold Layer
        movie_gold = spark.read.parquet(str(gold_db / "movies_gold"))
        movie_genre_gold = spark.read.parquet(str(gold_db / "movie_genre_gold"))


        # Identify Hidden Gems
        hidden_gems = (
            movie_gold
                .filter((col("rating_count") < 50) & (col("avg_rating") > 4.0))
                .select("movieId", "movieTitle", "avg_rating", "rating_count")
                .orderBy(col("avg_rating").desc(), col("rating_count").desc())
        )

        print("\n=== Hidden Gems with Rating Count < 50 and Avg Rating > 4.0) ===")

        hidden_gems.show(10, truncate=False)


        # Overall genre distribution
        overall_genre_dist = (
            movie_genre_gold
                .groupBy("genre")
                .agg(count("*").alias("overall_count"))
                .withColumn("overall_pct", round((col("overall_count") / lit(movie_genre_gold.count()) * 100), 2))
        )

        # Hidden gems genre distribution
        gems_with_genre = hidden_gems.join(movie_genre_gold.select("movieId", "genre"), "movieId")

        gems_genre_dist = (
            gems_with_genre
                .groupBy("genre")
                .agg(count("*").alias("gems_count"))
                .withColumn("gems_pct", round((col("gems_count") / lit(gems_with_genre.count()) * 100), 2))
        )

        # Compare both distributions
        comparison = overall_genre_dist.join(gems_genre_dist, "genre", "left").fillna(0)

        # Calculate difference distribution
        comparison = (
            comparison
                .withColumn("over_represented_diff", round(col("gems_pct") - col("overall_pct"), 2))
                .orderBy(col("over_represented_diff").desc())
        )


        print("\n=== Genres over-represented in hidden gems vs overall ===")
        
        (
        comparison
            .select(
                "genre", 
                "overall_pct", 
                "gems_pct", 
                "over_represented_diff"
            )
            .show(10, truncate=False)
        )
        
        return hidden_gems, comparison

    except Exception as e:
        print(f"Error in hidden gems analysis: {e}")
        raise


def select_inflight_catalog(spark: SparkSession, n: int = 100):

    # Simulate an "inflight catalog" constraint: if you could only offer 100 movies on a flight, 
    # which 100 would you pick to maximize expected passenger satisfaction across diverse tastes? 
    # Describe your selection criteria (e.g., genre coverage, rating quality, popularity balance) and implement it.

    try:
        # Read Gold tables
        movie_gold = spark.read.parquet(str(gold_db / "movies_gold"))
        movie_genre_gold = spark.read.parquet(str(gold_db / "movie_genre_gold"))

        # Filter movies with minimum credibility
        popularity = movie_gold.filter(col("rating_count") >= 10)

        # Calculate Bayesian Score to balance average rating and popularity
        catalog = popularity.withColumn(
            "score", 
            col("avg_rating") * log1p(col("rating_count"))
        )

        # Select Top N by score
        final_catalog = catalog.orderBy(col("score").desc()).limit(n)

        # === Genre Coverage Analysis ===
        catalog_with_genres = final_catalog.join(
            movie_genre_gold.select("movieId", "genre"), 
            "movieId"
        )

        genre_coverage = (catalog_with_genres
            .groupBy("genre")
            .agg(count("*").alias("movie_count"))
            .orderBy(col("movie_count").desc()))


        print(f"\n=== Inflight Catalog ===")

        print("\nGenre Coverage in Catalog:")
        genre_coverage.show(20, truncate=False)

        # Top N Recommended Movies
        print(f"Top {n} Recommended Movies for Inflight:")
        final_catalog.select(
            "movieTitle", 
            round(col("avg_rating"), 2).alias("avg_rating"), 
            "rating_count", 
            round(col("score"), 2).alias("score")
        ).show(n, truncate=False)

        return final_catalog, genre_coverage

    except Exception as e:
        print(f"Error in inflight catalog selection: {e}")
        raise
