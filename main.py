from sales_pipeline.bronze.ingestion import ingest_data
from sales_pipeline.silver.cleaning import clean_data
from sales_pipeline.gold.aggregation import aggregate_data
from sales_pipeline.utils.spark_session import get_spark_session

if __name__ == "__main__":
    spark = get_spark_session("SalesPipeline")
    print("LOG [main]: Spark session created")
    print("LOG [main]: Ingesting data...")
    ingest_data(spark)
    print('LOG [main]: Cleaning data...')
    clean_data(spark)
    print('LOG [main]: Aggregating data...')
    aggregate_data(spark)
    print('LOG [main]: Data pipeline completed')