import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame

from sales_pipeline.utils.utils import USD_EUR, JPY_EUR

def ca_per_month(spark: SparkSession) -> "DataFrame":
    """
    Calculer le chiffre d'affaires (en EUR) global par mois
    """

    df = spark.table("silver.silver_table")
    df = df.withColumn("Montant_Total_EUR", F.when(F.col("Devise") == "USD", F.col("Montant_Total") * USD_EUR).when(F.col("Devise") == "EUR", F.col("Montant_Total")).otherwise(F.col("Montant_Total") * JPY_EUR))
    df = df.withColumn("Month", F.month(F.to_date("Date_Vente", "dd/MM/yyyy"))).groupBy("Month").agg(F.sum("Montant_Total_EUR").alias("CA"))
    return df

def ca_per_month_shop(spark: SparkSession) -> "DataFrame":
    """
    Calculer le chiffre d'affaires (en EUR) par boutique et par mois
    """

    df = spark.table("silver.silver_table")
    df = df.withColumn("Montant_Total_EUR", F.when(F.col("Devise") == "USD", F.col("Montant_Total") * USD_EUR).when(F.col("Devise") == "EUR", F.col("Montant_Total")).otherwise(F.col("Montant_Total") * JPY_EUR))
    df = df.withColumn("Month", F.month(F.to_date("Date_Vente", "dd/MM/yyyy"))).groupBy('Nom_Boutique', "Month").agg(F.sum("Montant_Total_EUR").alias("CA"))
    return df

def top_selling_products(spark: SparkSession) -> "Dataframe":
    """
    Faire un classement global des produits les plus vendus
    """

    df = spark.table("silver.silver_table")
    df = df.groupBy('Nom_Produit').agg(F.sum("Quantité").alias("Quantite")).orderBy(F.desc("Quantite"))
    return df

def top_selling_products_by_CA(spark: SparkSession) -> "Dataframe":
    """
    Faire un classement global des produits qui ont généré le plus de chiffre d'affaires
    """

    df = spark.table("silver.silver_table")
    df = df.withColumn("Montant_Total_EUR", F.when(F.col("Devise") == "USD", F.col("Montant_Total") * USD_EUR).when(F.col("Devise") == "EUR", F.col("Montant_Total")).otherwise(F.col("Montant_Total") * JPY_EUR))
    df = df.groupBy('Nom_Produit').agg(F.sum("Montant_Total_EUR").alias("CA")).orderBy(F.desc("CA"))
    return df

def aggregate_data(
    spark: SparkSession,
    gold_schema: str = "workspace.gold"
    ) :
    """
    Fonction générale pour l'agrégation des données
    Fait les analyses et les stocke dans les tables gold
    """
    
    print("| LOG [aggregation]: Process revenue per month...", end="")
    df1 = ca_per_month(spark)
    df1.write.format("delta").mode("append").saveAsTable(f"{gold_schema}.revenue_per_month")
    print("Done")

    print("| LOG [aggregation]: Process revenue per month and shop...", end="")
    df2 = ca_per_month_shop(spark)
    df2.write.format("delta").mode("append").saveAsTable(f"{gold_schema}.revenue_per_month_and_shop")
    print("Done")
    
    print("| LOG [aggregation]: Process top selling products...", end="")
    df3 = top_selling_products(spark)
    df3.write.format("delta").mode("append").saveAsTable(f"{gold_schema}.top_selling_products")
    print("Done")
    
    print("| LOG [aggregation]: Process top selling products by revenue...", end="")
    df4 = top_selling_products_by_CA(spark)
    df4.write.format("delta").mode("append").saveAsTable(f"{gold_schema}.top_selling_products_by_revenue")
    print("Done")
    