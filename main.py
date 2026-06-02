from src import (
    create_spark_session,
    trigger_bronze,
    trigger_silver,
    trigger_gold,
    analyze_top_genres,
    find_hidden_gems,
    select_inflight_catalog
)

def main():
    spark = create_spark_session()
    
    try:
        print("## Running PySpark Medallion Pipeline...")
        print("------------------------------------")
    
        trigger_bronze(spark)
        trigger_silver(spark)
        trigger_gold(spark)
        
        print("## Medallion Pipeline completed successfully!")
        print("------------------------------------\n")

        print("## Analyzing data...")
        print("------------------------------------\n")

        # Analyze top genres
        analyze_top_genres(spark)

        # Find hidden gems
        find_hidden_gems(spark)

        # Select inflight catalog
        select_inflight_catalog(spark, n=100)
        
        print("\n## Analyzing data completed.")
        print("------------------------------------\n")
    
        print("##### All Processes completed successfully!\n")

        print("Spark Web UI is available at: http://localhost:4040")
        input("Press Enter to stop Spark Session and exit...\n")
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

