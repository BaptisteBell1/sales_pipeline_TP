import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()


def test_clean_data_removes_nulls(spark):
    # Vérifie que la fonction de nettoyage supprime bien les lignes vides
    df = spark.table("silver.silver_table")

    for col in df.columns:
        null_count = df.filter(df[col].isNull()).count()
        assert null_count == 0, f"Colonne {col} contient des NULL"
