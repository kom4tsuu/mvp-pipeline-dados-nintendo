# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Silver — Limpeza e Padronização
# MAGIC **MVP: Pipeline de Dados na Nuvem — Nintendo Games**
# MAGIC
# MAGIC Nesta etapa, partimos de `bronze.jogos_raw` e entregamos `silver.jogos_limpos`: dados tipados,
# MAGIC deduplicados, padronizados e prontos para modelagem. Cada transformação abaixo está documentada
# MAGIC com o "o quê" e o "porquê" — use esses comentários na seção "Pipeline de Dados (Etapa 4.4)" do README.

# COMMAND ----------

CATALOG = "nintendo_games"
spark.sql(f"USE CATALOG {CATALOG}")

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

df = spark.table(f"{CATALOG}.bronze.jogos_raw")
print("Linhas na Bronze:", df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Remoção de duplicatas
# MAGIC A chave natural de um lançamento é `title` + `platform` (o mesmo jogo pode existir em
# MAGIC plataformas diferentes, mas não deveria se repetir na mesma plataforma).

# COMMAND ----------

dup_count = df.count() - df.dropDuplicates(["title", "platform"]).count()
print(f"Duplicatas removidas (title+platform): {dup_count}")

df = df.dropDuplicates(["title", "platform"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Padronização da coluna `platform`
# MAGIC Identificamos um valor sujo (`TG16)`, resquício de parsing da fonte original) que não
# MAGIC corresponde a nenhuma plataforma real da Nintendo. Como é 1 único registro em 1094,
# MAGIC optamos por descartá-lo (não é possível inferir com segurança a plataforma correta) e
# MAGIC documentar a decisão, em vez de tentar adivinhar o valor.

# COMMAND ----------

df = df.withColumn("platform", F.trim(F.col("platform")))
linhas_antes = df.count()
df = df.filter(F.col("platform") != "TG16)")
print(f"Registros descartados por plataforma inválida: {linhas_antes - df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tratamento da coluna `date`
# MAGIC A coluna traz datas reais (`"Nov 17, 2023"`) e também valores não-data (`TBA`, `Canceled`,
# MAGIC `TBA 2024`, `Q4 2015`), que representam jogos sem lançamento confirmado. Em vez de descartar
# MAGIC essas linhas (perderíamos informação de catálogo), criamos uma coluna de status e convertemos
# MAGIC apenas o que é de fato uma data para `DateType`; o resto vira `NULL` em `release_date`.

# COMMAND ----------

df = df.withColumn(
    "release_status",
    F.when(F.col("date") == "Canceled", "Cancelado")
     .when(F.col("date").rlike("^TBA"), "A anunciar")
     .otherwise("Lancado")
)

df = df.withColumn(
    "release_date",
    F.when(F.col("release_status") == "Lancado", F.to_date("date", "MMM d, yyyy"))
     .otherwise(F.lit(None).cast("date"))
)

df = df.withColumn("release_year", F.year("release_date"))

df.groupBy("release_status").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tipagem correta de `meta_score` e `user_score`
# MAGIC `meta_score` (nota da crítica, escala 0-100) e `user_score` (nota do usuário, escala 0-10)
# MAGIC vêm como string no bruto. Convertemos para numérico; strings vazias viram `NULL`
# MAGIC (ausência real da nota, não um erro).

# COMMAND ----------

df = (
    df.withColumn("meta_score", F.col("meta_score").cast(DoubleType()))
      .withColumn("user_score", F.col("user_score").cast(DoubleType()))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Parsing de `genres` e `developers`
# MAGIC Essas colunas vêm como strings representando listas Python (ex.: `"['Action', 'Platformer']"`).
# MAGIC Convertemos para arrays reais do Spark, o que permite explodir em tabelas-ponte na camada Gold
# MAGIC (um jogo pode ter múltiplos gêneros e múltiplas desenvolvedoras).

# COMMAND ----------

df = df.withColumn(
    "genres_array",
    F.transform(
        F.split(F.regexp_replace(F.col("genres"), r"[\[\]']", ""), ","),
        lambda x: F.trim(x)
    )
).withColumn("genres_array", F.filter("genres_array", lambda x: x != ""))

df = df.withColumn(
    "developers_array",
    F.transform(
        F.split(F.regexp_replace(F.coalesce(F.col("developers"), F.lit("")), r"[\[\]']", ""), ","),
        lambda x: F.trim(x)
    )
).withColumn("developers_array", F.filter("developers_array", lambda x: x != ""))

df = df.withColumn("primary_genre", F.col("genres_array")[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Padronização de `esrb_rating`
# MAGIC Valores nulos são mantidos como `"Nao informado"` em vez de `NULL` puro, para deixar
# MAGIC explícito na análise que a ausência é da fonte de dados (não um erro de pipeline).

# COMMAND ----------

df = df.withColumn(
    "esrb_rating",
    F.when(F.col("esrb_rating").isNull() | (F.trim(F.col("esrb_rating")) == ""), "Nao informado")
     .otherwise(F.trim(F.col("esrb_rating")))
)

# COMMAND ----------

# MAGIC %md ## 7. Seleção final e persistência da Silver

# COMMAND ----------

df_silver = df.select(
    "title", "platform", "release_date", "release_year", "release_status",
    "meta_score", "user_score", "esrb_rating",
    "primary_genre", "genres_array", "developers_array",
    "link", "_source_file", "_ingestion_timestamp"
)

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.jogos_limpos")
)

print("Tabela silver.jogos_limpos criada. Linhas:", df_silver.count())
display(df_silver.limit(10))
