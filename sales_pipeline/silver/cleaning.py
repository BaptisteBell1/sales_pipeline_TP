import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from sales_pipeline.utils.utils import (
    cities,
    columns_name,
    devise_dict,
    pays_dict,
    date_format_dict 
)

def clean_data(spark: SparkSession,
            bronze_schema: str = "workspace.bronze",
            silver_schema: str = "workspace.silver",
            silver_table: str = "silver_table",
            catalogue_table: str = "catalogue"):
    
    """
    Nettoyage et filtrage de la donnée. Certaines données doivent être modifiées / ajoutées:
    - ID_Vente (int) : Identifiant unique de la vente
    - Date_Vente (date) : Date de la vente au format dd/mm/aaaa
    - Devise (string) : Devise de vente de la boutique (USD, EUR ou JPY)
    - Nom_Boutique (string) : Nom de la boutique
    - Ville (string) : Ville où se trouve la boutique
    - Pays (string) : Pays où se trouve la boutique (USA, France, Japon)
    Les noms des colonnes doivent être en français et chaque colonne doit être typé correctement.
    """

    dfs = []
    df_catalogue = spark.table(f"{bronze_schema}.{catalogue_table}")
    for city in cities:
        df = spark.table(f"{bronze_schema}.{city}")
        
        for old, new in zip(df.columns, columns_name):
            df = df.withColumnRenamed(old, new)
        
        if (city != 'paris'):
            df = df.join(df_catalogue, how='inner', on=df.Nom_Produit == df_catalogue.Nom_Produit_Anglais)
            df = df.drop('Nom_Produit', 'Nom_Produit_Anglais', 'Catégorie_Anglais', 'Catégorie')
        
        df = df.withColumnRenamed('Catégorie_Francais', 'Catégorie').withColumnRenamed('Nom_Produit_Francais', 'Nom_Produit')

        df = df.withColumn("Devise", F.lit(devise_dict[city]))\
            .withColumn("Nom_Boutique", F.lit('Boutique_' + city))\
            .withColumn("Ville", F.lit(city))\
            .withColumn("Pays", F.lit(pays_dict[city]))
        
        df = df.select(*columns_name, "Devise", "Nom_Boutique", "Ville", "Pays")
        
        df = df.withColumn("Date_Vente", F.to_date("Date_Vente", date_format_dict[city]))\
            .withColumn("Date_Vente",F.date_format("Date_Vente", "dd/MM/yyyy"))
        
        dfs.append(df)

    df = dfs[0].union(dfs[1]).union(dfs[2])
    df = df.withColumn("ID_Vente", F.monotonically_increasing_id())

    df.write.format("delta").mode("append").saveAsTable(f"{silver_schema}.{silver_table}")