import os
import re
import shutil

from pyspark.sql import SparkSession

def ingest_data(
    spark: SparkSession,
    dir_path: str = "/Volumes/workspace/raw_data/raw_data",
    archives_path: str = "/Volumes/workspace/raw_data/archives/"):

    """
    - Pour chaque boutique, les données reçues doivent être stocké dans une table. On aura donc trois tables, une par boutique.
    - Les données ne doivent pas être altérées, ils doivent être mis à disposition dans le même état qu'ils ont été reçues.
    - Les fichiers reçus doivent être archivé une fois ingéré dans une table.
    """
    
    for files in os.listdir(dir_path):
        if os.path.isdir(os.path.join(dir_path, files)):
            match = re.search(r"boutique_(.+)", files)
            city = match.group(1) if match else None
            
            print("| LOG [ingestion]: Process Directory: " + city + "...")
            
            file_path = os.path.join(dir_path, files)
            for city_files in os.listdir(file_path):
                print(f"|___ LOG [ingestion]: Process file: {city_files}...", end="")
                
                file_path2 =  os.path.join(file_path, city_files)
                df = spark.read.option("header", "true").csv(file_path2)

                df.write.format("delta").mode("append").saveAsTable(f"workspace.bronze.{city}")

                shutil.move(file_path2, os.path.join(archives_path, os.path.basename(file_path2)))

                print("Done")
        else:
            print("| LOG [ingestion]: Process catalogue...", end="")
            catalogue_file = os.path.join(dir_path, files)
            df = spark.read.option("header", "true").csv(catalogue_file)
            df.write.format("delta").mode("append").saveAsTable(f"workspace.bronze.catalogue")
            shutil.move(catalogue_file, os.path.join(archives_path, os.path.basename(catalogue_file)))
            print("Done")