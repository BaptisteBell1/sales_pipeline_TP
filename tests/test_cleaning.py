import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()


def test_clean_data_removes_nulls(spark: SparkSession):
    # Vérifie que la fonction de nettoyage supprime bien les lignes vides
    df = spark.table("silver.silver_table")
    cols = []

    for col in df.columns:
        null_count = df.filter(df[col].isNull()).count()
        if null_count > 0:
            cols.append(col)
    
    assert not cols, f"Les colonnes {cols} contiennent des NULL"

def Bronze_check_correct_columns(spark: SparkSession):
    """
    Check if the bronze tables have the correct columns
    """
    tables = [row.tableName for row in tables_df.collect()]
    invalids_tables = []
    
    columns = {
        "columns_paris": ["ID_Vente", "Date_Vente", "Nom_Produit", "Catégorie", "Prix_Unitaire", "Quantité", "Montant_Total"],
        "columns_other": ["ID_Sale", "Sale_Date", "Product_Name", "Category", "Unit_Price", "Quantity", "Total_Amount"],
    }

    for table in tables:
        if table == 'catalogue' :
            continue
        
        df = spark.table(f"bronze.{table}")
        actual_columns = df.columns

        if table == "paris":
            expected = expected_columns["paris"]
        
        else:
            expected = expected_columns["other"]

        if set(actual_columns) != set(expected):
            invalid_tables.append(
                f"{table} -> expected={expected}, got={actual_columns}"
            )

    assert not invalid_tables, (
        "Colonnes invalides dans les tables bronze:\n" +
        "\n".join(invalid_tables))
    

def Silver_no_null(spark: SparkSession):
    """
    Check if the silver tables have no null values
    """
    df = spark.table("silver.silver_table")
    cols = []
    
    for col in df.columns:
        null_count = df.filter(df[col].isNull()).count()
        if null_count > 0:
            cols.append(col)

    assert not cols, f"Les colonnes {cols} contiennent des NULL"

def Silver_correct_date_format(spark: SparkSession):
    """
    Check if the silver tables have the correct date format
    Format : dd/MM/yyyy
    """
    df = spark.table("silver.silver_table")
    df = df.select("Date_Vente")
    
    invalid_count = (
        df
        .withColumn(
            "parsed_date",
            to_date(col("Date_Vente"), "dd/MM/yyyy")
        )
        .filter(col("parsed_date").isNull() & col("Date_Vente").isNotNull())
        .count()
    )

    assert invalid_count == 0, (
        f"{invalid_count} lignes ont un format de date invalide (dd/MM/yyyy)"
    )
    
def Silver_correct_columns(spark: SparkSession):
    """
    Check if the silver tables have the correct columns
    """
    expected_columns = [
        "ID_Vente", "Date_Vente", "Nom_Produit", "Catégorie", "Prix_Unitaire", "Quantité","Montant_Total", "Devise", "Nom_Boutique", "Ville", "Pays"
    ]
    df = spark.table("silver.silver_table")
    actual_columns = df.columns
    
    assert set(actual_columns) == set(expected_columns), (
        f"Colonnes incorrectes.\n"
        f"Expected: {expected_columns}\n"
        f"Got: {actual_columns}"
    )

def Silver_correct_Product_and_Category_name(spark: SparkSession):
    """
    Check if the silver tables have the correct Product and Category name (French)
    """
    silver = spark.table("workspace.bronze.paris")
    catalogue = spark.table("workspace.bronze.catalogue")

    df = silver.join(catalogue, ventes["Nom_Produit"] == catalogue["Produit_Français"],"left")

    invalid_products = df.filter(col("Produit_Français").isNull()).count()

    assert invalid_products == 0, (
        f"{invalid_products} produits ne sont pas en français ou absents du catalogue"
    )