# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Bronze — Ingestão dos Dados Brutos
# MAGIC **MVP: Pipeline de Dados na Nuvem — Nintendo Games**
# MAGIC
# MAGIC Objetivo desta etapa: trazer o arquivo `NintendoGames.csv` (fonte: dataset "Nintendo Games",
# MAGIC scraped de metacritic.com, disponível no Kaggle sob licença CC0: Public Domain) para dentro do
# MAGIC ambiente Databricks e persistir exatamente como veio, sem nenhuma transformação, apenas com
# MAGIC metadados de controle de ingestão (rastreabilidade).
# MAGIC
# MAGIC **Antes de rodar:** faça upload do `NintendoGames.csv` para um Volume do Unity Catalog.
# MAGIC No Databricks Free Edition: Catalog (ícone à esquerda) → seu catálogo → schema `bronze` →
# MAGIC Volumes → criar volume `raw_files` → Upload to this volume.

# COMMAND ----------

# MAGIC %md ## 1. Configuração do catálogo e schemas

# COMMAND ----------

CATALOG = "nintendo_games"          # ajuste para o nome do seu catálogo no Unity Catalog
VOLUME_PATH = f"/Volumes/{CATALOG}/bronze/raw_files/NintendoGames.csv"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md ## 2. Leitura do CSV bruto
# MAGIC Lemos tudo como string por enquanto — tipagem correta é responsabilidade da camada Silver.
# MAGIC Isso preserva o dado exatamente como chegou (princípio da camada Bronze).

# COMMAND ----------

df_bronze_raw = (
    spark.read
    .option("header", True)
    .option("multiLine", True)
    .option("escape", '"')
    .csv(VOLUME_PATH)
)

print(f"Linhas lidas: {df_bronze_raw.count()}")
display(df_bronze_raw)

# COMMAND ----------

# MAGIC %md ## 3. Adição de metadados de controle (linhagem/rastreabilidade)

# COMMAND ----------

from pyspark.sql import functions as F

df_bronze = (
    df_bronze_raw
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.lit("NintendoGames.csv"))
    .withColumn("_source_origin", F.lit("Kaggle - Nintendo Games Dataset (scraped from metacritic.com), licenca CC0: Public Domain"))
)

# COMMAND ----------

# MAGIC %md ## 4. Persistência como tabela Delta (Bronze)

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.bronze.jogos_raw")
)

print("Tabela bronze.jogos_raw criada com sucesso.")
display(spark.sql(f"SELECT * FROM {CATALOG}.bronze.jogos_raw LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Evidência para o README

# COMMAND ----------

print("Contagem de linhas na Bronze:", spark.table(f"{CATALOG}.bronze.jogos_raw").count())
spark.sql(f"DESCRIBE TABLE {CATALOG}.bronze.jogos_raw").show(30, truncate=False)
