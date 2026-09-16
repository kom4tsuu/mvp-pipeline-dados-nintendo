# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Gold — Modelagem Dimensional (Esquema Estrela)
# MAGIC **MVP: Pipeline de Dados na Nuvem — Nintendo Games**
# MAGIC
# MAGIC A partir de `silver.jogos_limpos`, construímos um Esquema Estrela:
# MAGIC - **Fato:** `gold.fato_jogos` — grão = um lançamento (jogo em uma plataforma).
# MAGIC - **Dimensões:** `dim_plataforma`, `dim_genero`, `dim_classificacao_etaria`, `dim_data`.
# MAGIC - **Tabelas-ponte** (relação N:N): `ponte_jogo_genero`, `ponte_jogo_desenvolvedora`
# MAGIC   — um jogo pode ter vários gêneros e várias desenvolvedoras, o que não cabe em uma
# MAGIC   dimensão tradicional sem duplicar a linha de fato.

# COMMAND ----------

CATALOG = "nintendo_games"
spark.sql(f"USE CATALOG {CATALOG}")

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df = spark.table(f"{CATALOG}.silver.jogos_limpos")
df = df.withColumn("jogo_id", F.monotonically_increasing_id())

# COMMAND ----------

# MAGIC %md ## 1. dim_plataforma

# COMMAND ----------

dim_plataforma = (
    df.select("platform").distinct()
    .withColumn("plataforma_id", F.row_number().over(Window.orderBy("platform")))
    .withColumnRenamed("platform", "nome_plataforma")
    .select("plataforma_id", "nome_plataforma")
)
dim_plataforma.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gold.dim_plataforma")
display(dim_plataforma)

# COMMAND ----------

# MAGIC %md ## 2. dim_genero (gênero primário do jogo)

# COMMAND ----------

dim_genero = (
    df.select("primary_genre").distinct()
    .withColumn("genero_id", F.row_number().over(Window.orderBy("primary_genre")))
    .withColumnRenamed("primary_genre", "nome_genero")
    .select("genero_id", "nome_genero")
)
dim_genero.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gold.dim_genero")
display(dim_genero)

# COMMAND ----------

# MAGIC %md ## 3. dim_classificacao_etaria

# COMMAND ----------

esrb_map = {
    "E": "Livre para todos",
    "E10+": "Livre para maiores de 10 anos",
    "T": "Adolescentes (13+)",
    "M": "Maduro (17+)",
    "RP": "Classificacao pendente",
    "Nao informado": "Nao informado pela fonte",
}
mapping_expr = F.create_map([F.lit(x) for pair in esrb_map.items() for x in pair])

dim_classificacao = (
    df.select("esrb_rating").distinct()
    .withColumn("classificacao_id", F.row_number().over(Window.orderBy("esrb_rating")))
    .withColumn("descricao", mapping_expr[F.col("esrb_rating")])
    .withColumnRenamed("esrb_rating", "sigla_esrb")
    .select("classificacao_id", "sigla_esrb", "descricao")
)
dim_classificacao.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gold.dim_classificacao_etaria")
display(dim_classificacao)

# COMMAND ----------

# MAGIC %md ## 4. dim_data (granularidade: dia de lançamento)

# COMMAND ----------

dim_data = (
    df.select("release_date").filter(F.col("release_date").isNotNull()).distinct()
    .withColumn("data_id", F.date_format("release_date", "yyyyMMdd").cast("int"))
    .withColumn("ano", F.year("release_date"))
    .withColumn("mes", F.month("release_date"))
    .withColumn("trimestre", F.quarter("release_date"))
    .withColumn("nome_mes", F.date_format("release_date", "MMMM"))
    .select("data_id", "release_date", "ano", "mes", "trimestre", "nome_mes")
    .withColumnRenamed("release_date", "data_completa")
)
dim_data.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gold.dim_data")
display(dim_data.limit(10))

# COMMAND ----------

# MAGIC %md ## 5. fato_jogos

# COMMAND ----------

fato_jogos = (
    df.join(dim_plataforma, df.platform == dim_plataforma.nome_plataforma, "left")
      .join(dim_genero, df.primary_genre == dim_genero.nome_genero, "left")
      .join(dim_classificacao, df.esrb_rating == dim_classificacao.sigla_esrb, "left")
      .withColumn("data_id", F.date_format("release_date", "yyyyMMdd").cast("int"))
      .select(
          "jogo_id", "title", "plataforma_id", "genero_id", "classificacao_id", "data_id",
          "meta_score", "user_score", "release_status", "release_year", "link"
      )
      .withColumnRenamed("title", "titulo_jogo")
)

fato_jogos.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gold.fato_jogos")
print("fato_jogos:", fato_jogos.count(), "linhas")
display(fato_jogos.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Tabelas-ponte (relações N:N)
# MAGIC Um jogo pode ter mais de um gênero e mais de uma desenvolvedora — por isso não cabem
# MAGIC como atributo único do fato. Modelamos como tabelas-ponte clássicas de Esquema Estrela.

# COMMAND ----------

ponte_genero = (
    df.select("jogo_id", F.explode("genres_array").alias("nome_genero"))
      .join(dim_genero, "nome_genero", "left")
      .select("jogo_id", "genero_id")
)
ponte_genero.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gold.ponte_jogo_genero")

dim_desenvolvedora = (
    df.select(F.explode("developers_array").alias("nome_desenvolvedora")).distinct()
      .withColumn("desenvolvedora_id", F.row_number().over(Window.orderBy("nome_desenvolvedora")))
      .select("desenvolvedora_id", "nome_desenvolvedora")
)
dim_desenvolvedora.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gold.dim_desenvolvedora")

ponte_desenvolvedora = (
    df.select("jogo_id", F.explode("developers_array").alias("nome_desenvolvedora"))
      .join(dim_desenvolvedora, "nome_desenvolvedora", "left")
      .select("jogo_id", "desenvolvedora_id")
)
ponte_desenvolvedora.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gold.ponte_jogo_desenvolvedora")

print("Tabelas-ponte e dim_desenvolvedora criadas com sucesso.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Evidência para o README
# MAGIC Tire screenshot da aba **Catalog → nintendo_games → gold** mostrando todas as tabelas
# MAGIC (fato_jogos, dim_plataforma, dim_genero, dim_classificacao_etaria, dim_data,
# MAGIC dim_desenvolvedora, ponte_jogo_genero, ponte_jogo_desenvolvedora) persistidas.

# COMMAND ----------

for t in ["fato_jogos", "dim_plataforma", "dim_genero", "dim_classificacao_etaria",
          "dim_data", "dim_desenvolvedora", "ponte_jogo_genero", "ponte_jogo_desenvolvedora"]:
    print(t, "->", spark.table(f"{CATALOG}.gold.{t}").count(), "linhas")
