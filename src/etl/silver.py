from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, split, row_number, to_timestamp, from_unixtime, year, month, regexp_extract, trim, when
from pathlib import Path

bronze_db = Path("database/bronze")
silver_db = Path("database/silver")

# Create folders if they don't exist
bronze_db.mkdir(parents=True, exist_ok=True)
silver_db.mkdir(parents=True, exist_ok=True)

def trigger_silver(spark: SparkSession):

    print("\tStarting Silver Layer...")
    
    try:
        # Read and cleanse data from Bronze Delta
        df_links = spark.read.format("parquet").load(str(bronze_db / "links"))
        df_links_clean = clean_links_dataframe(df_links)

        # Read and cleanse data from Bronze Delta
        df_movies = spark.read.format("parquet").load(str(bronze_db / "movies"))
        df_movies_clean = clean_movies_dataframe(df_movies)

        # Read and cleanse data from Bronze Delta
        df_ratings = spark.read.format("parquet").load(str(bronze_db / "ratings"))
        df_ratings_clean = clean_ratings_dataframe(df_ratings)

        # Read and cleanse data from Bronze Delta
        df_tags = spark.read.format("parquet").load(str(bronze_db / "tags"))
        df_tags_clean = clean_tags_dataframe(df_tags)

        # Store clean dataframe into Silver layer
        df_list = [df_links_clean, df_movies_clean, df_ratings_clean, df_tags_clean]
        store_to_silver(df_list)

    except Exception as e:
        print(f"Error in Silver layer: {e}\n")
        raise


def clean_links_dataframe(df):
    try:
        print(f"\t\tProfiling Links DataFrame...")

        # Get total records of missing values
        total_missing_values = df.filter(
            col("movieId").isNull() & col("imdbId").isNull()
        ).count()

        print(f"\t\tTotal missing values in Links DataFrame: {total_missing_values}")


        # Get total records of duplicate values
        total_rows = df.count()
        unique_rows = df.select("movieId", "imdbId").distinct().count()
        total_duplicates = total_rows - unique_rows

        print(f"\t\tTotal duplicates in Links DataFrame: {total_duplicates}")


        # Remove missing values
        df = df.dropna(how="all", subset=["movieId", "imdbId"])

        # 1. Define the window: Group by your IDs and sort by the target column descending
        window_spec = Window.partitionBy("movieId", "imdbId").orderBy(col("tmdbId").cast("int").desc())

        # 2. Assign ranks, keep only the top row (rank 1), and clean up the helper column
        df = (
            df.withColumn("row_num", row_number().over(window_spec))
              .filter(col("row_num") == 1)
              .drop("row_num")
        )

        clean_df = (
            df
            .withColumn("movieId", col("movieId").cast("int"))
            .withColumn("imdbId", col("imdbId").cast("int"))
            .withColumn("tmdbId", col("tmdbId").cast("int"))
        )

        print(f"\t\tClean Links DataFrame Completed\n")

        return {"links": clean_df}
    
    except Exception as e:
        print(f"Error in clean_links_dataframe: {e}\n")
        raise


def clean_movies_dataframe(df):
    try:
        print(f"\t\tProfiling Movies DataFrame...")

        # Get total records of missing values
        total_missing_values = df.filter(
            col("movieId").isNull() & col("title").isNull()
        ).count()

        print(f"\t\tTotal missing values in Movies DataFrame: {total_missing_values}")


        # Get total records of duplicate values
        total_rows = df.count()
        unique_rows = df.select("movieId").distinct().count()
        total_duplicates = total_rows - unique_rows

        print(f"\t\tTotal duplicates in Movies DataFrame: {total_duplicates}")


        # Remove missing values
        df = df.dropna(how="all", subset=["movieId", "title"])
        df = df.dropDuplicates(subset=["movieId"])


        # Regex pattern for checking and extracting the year at the end
        year_pattern = r"\((\d{4})\)$"
        full_pattern = r"(.*?)\s*\((\d{4})\)$"

        clean_df = (
            df
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

        print(f"\t\tClean Movies DataFrame Completed\n")

        return {"movies": clean_df}
    
    except Exception as e:
        print(f"Error in clean_movies_dataframe: {e}\n")
        raise


def clean_ratings_dataframe(df):
    try:
        print(f"\t\tProfiling Ratings DataFrame...")

        # Get total records of missing values
        total_missing_values = df.filter(
            col("userId").isNull() & col("movieId").isNull()
        ).count()

        print(f"\t\tTotal missing values in Ratings DataFrame: {total_missing_values}")


        # Get total records of duplicate values
        total_rows = df.count()
        unique_rows = df.select("userId", "movieId").distinct().count()
        total_duplicates = total_rows - unique_rows

        print(f"\t\tTotal duplicates in Ratings DataFrame: {total_duplicates}")


        # Remove missing values
        df = df.dropna(how="all", subset=["userId", "movieId"])

        # 1. Define the window: Group by your IDs and sort by the target column descending
        window_spec = Window.partitionBy("userId", "movieId").orderBy(col("timestamp").cast("int").desc())

        # 2. Assign ranks, keep only the top row (rank 1), and clean up the helper column
        df = (
            df.withColumn("row_num", row_number().over(window_spec))
              .filter(col("row_num") == 1)
              .drop("row_num")
        )

        clean_df = (
            df
            .withColumn("datetime", from_unixtime(col("timestamp").cast("long")))
            .withColumn("rating_year", year(col("datetime")))
            .withColumn("rating_month", month(col("datetime")))
            .withColumn("userId", col("userId").cast("int"))
            .withColumn("movieId", col("movieId").cast("int"))
            .withColumn("rating", col("rating").cast("float"))
        )

        print(f"\t\tClean Ratings DataFrame Completed\n")

        return {"ratings": clean_df}
    
    except Exception as e:
        print(f"Error in clean_ratings_dataframe: {e}\n")
        raise


def clean_tags_dataframe(df):
    try:
        print(f"\t\tProfiling Tags DataFrame...")

        # Get total records of missing values
        total_missing_values = df.filter(
            col("tag").isNull()
        ).count()

        print(f"\t\tTotal missing values in Tags DataFrame: {total_missing_values}")


        # Get total records of duplicate values
        total_rows = df.count()
        unique_rows = df.select("userId", "movieId", "tag").distinct().count()
        total_duplicates = total_rows - unique_rows

        print(f"\t\tTotal duplicates in Tags DataFrame: {total_duplicates}")


        # Remove missing values
        df = df.dropna(how="all", subset=["userId", "movieId", "tag"])

        # 1. Define the window: Group by your IDs and sort by the target column descending
        window_spec = Window.partitionBy("userId", "movieId", "tag").orderBy(col("timestamp").cast("int").desc())

        # 2. Assign ranks, keep only the top row (rank 1), and clean up the helper column
        df = (
            df.withColumn("row_num", row_number().over(window_spec))
              .filter(col("row_num") == 1)
              .drop("row_num")
        )

        clean_df = (
            df
            .withColumn("userId", col("userId").cast("int"))
            .withColumn("movieId", col("movieId").cast("int"))
            .withColumn("datetime", from_unixtime(col("timestamp").cast("long")))
            .withColumn("tags_year", year(col("datetime")))
            .withColumn("tags_month", month(col("datetime")))
        )

        print(f"\t\tClean Tags DataFrame Completed\n")

        return {"tags": clean_df}
    
    except Exception as e:
        print(f"Error in clean_tags_dataframe: {e}\n")
        raise


def store_to_silver(df_list):
    try:

        for result in df_list:
            [(key, value)] = result.items()
            value.write.format("parquet").mode("overwrite").save(str(silver_db / key))
            print(f"\tStore to Silver Layer => {key.capitalize()}: {value.count():,}")

        print(f"\tSilver Layer completed successfully\n")

    except Exception as e:
        print(f"Error in store_to_silver: {e}\n")
        raise
