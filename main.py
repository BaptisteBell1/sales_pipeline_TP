from sales_pipeline.bronze.ingestion import ingest_data
from sales_pipeline.silver.cleaning import clean_data
from sales_pipeline.gold.aggregation import aggregate_data
from sales_pipeline.utils.spark_session import get_spark_session

if __name__ == "__main__":
    spark = get_spark_session("SalesPipeline")
    df_bronze = ingest_data(spark)
    df_silver = clean_data(df_bronze)
    df_gold = aggregate_data(df_silver)