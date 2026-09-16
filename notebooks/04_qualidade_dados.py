# Databricks notebook source
# MAGIC %md
# MAGIC # Qualidade de Dados
# MAGIC **MVP: Pipeline de Dados na Nuvem — Nintendo Games**
# MAGIC
# MAGIC Avaliação de completude, consistência, unicidade, acurácia e outliers sobre o dado bruto
# MAGIC (`bronze.jogos_raw`), documentando o que foi encontrado e como cada problema foi tratado na
# MAGIC camada Silver (notebook `02_silver_transformacao`).

# COMMAND ----------

CATALOG = "nintendo_games"
spark.sql(f"USE CATALOG {CATALOG}")

from pyspark.sql import functions as F

df = spark.table(f"{CATALOG}.bronze.jogos_raw")
total = df.count()
print("Total de registros brutos:", total)

# COMMAND ----------

# MAGIC %md ## 1. Completude — valores nulos/vazios por coluna

# COMMAND ----------

nulos = df.select([
    F.count(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), c)).alias(c)
    for c in ["meta_score", "user_score", "esrb_rating", "developers", "title", "platform", "date", "genres"]
])
display(nulos)

# COMMAND ----------

# MAGIC %md
# MAGIC **Achados (calculados sobre os 1094 registros):**
# MAGIC - `meta_score`: 385 nulos (35,2%) — muitos jogos, sobretudo mais antigos ou de nicho, nunca
# MAGIC   receberam nota consolidada da crítica no Metacritic.
# MAGIC - `user_score`: 238 nulos (21,7%) — jogos sem volume suficiente de avaliações de usuários.
# MAGIC - `esrb_rating`: 122 nulos (11,2%) — jogos sem classificação etária cadastrada na fonte.
# MAGIC - `developers`: 3 nulos.
# MAGIC - `title`, `platform`, `date`, `genres`: sem nulos.
# MAGIC
# MAGIC **Tratamento:** nulos em `meta_score`/`user_score` foram mantidos como `NULL` (ausência real
# MAGIC da nota, não um erro — forçar um valor como 0 distorceria qualquer média). Nulos em
# MAGIC `esrb_rating` foram padronizados para o rótulo `"Nao informado"` na Silver, para ficarem
# MAGIC explícitos nas análises em vez de somem como `NULL` silencioso.

# COMMAND ----------

# MAGIC %md ## 2. Unicidade — duplicatas

# COMMAND ----------

duplicatas = df.groupBy("title", "platform").count().filter("count > 1")
print("Pares (title, platform) duplicados:", duplicatas.count())
display(duplicatas)

# COMMAND ----------

# MAGIC %md
# MAGIC **Achado:** 2 pares duplicados de `title`+`platform`. **Tratamento:** removidos via
# MAGIC `dropDuplicates(["title","platform"])` na Silver, mantendo a primeira ocorrência.

# COMMAND ----------

# MAGIC %md ## 3. Consistência — formato de `platform` e `date`

# COMMAND ----------

print("Valores distintos de platform:")
df.groupBy("platform").count().orderBy(F.desc("count")).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Achado:** o valor `TG16)` aparece 1 vez e não corresponde a nenhuma plataforma Nintendo
# MAGIC válida (resíduo de parsing da fonte original). **Tratamento:** registro descartado na Silver,
# MAGIC com a decisão documentada (não é seguro inferir a plataforma correta a partir de 1 registro).

# COMMAND ----------

nao_data = df.filter(~F.col("date").rlike(r"^[A-Za-z]{3} \d{1,2}, \d{4}$"))
print("Registros com 'date' fora do padrão MMM d, yyyy:", nao_data.count())
nao_data.groupBy("date").count().orderBy(F.desc("count")).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Achado:** 30 registros com `date` fora do padrão (`TBA`, `Canceled`, `TBA 2024`,
# MAGIC `TBA 2011`, `TBA 2010`, `Q4 2015`) — representam jogos anunciados mas não lançados, ou
# MAGIC cancelados. **Tratamento:** criada a coluna `release_status` (Lancado / A anunciar /
# MAGIC Cancelado) na Silver; `release_date` fica `NULL` para os que não têm data real, preservando
# MAGIC a informação em vez de descartar a linha inteira.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Conversão numérica para fins de análise
# MAGIC A tabela `bronze.jogos_raw` guarda `meta_score` e `user_score` como texto (`StringType`) de
# MAGIC propósito — é o princípio da camada Bronze: preservar o dado exatamente como chegou, sem
# MAGIC tipagem. A conversão de tipo "de verdade" (permanente) acontece na Silver. Aqui, só para
# MAGIC calcular estatísticas (mínimo, máximo, quartis), criamos colunas numéricas auxiliares
# MAGIC dentro deste notebook — sem alterar a tabela Bronze original.

# COMMAND ----------

df = (
    df.withColumn("meta_score_num", F.col("meta_score").cast("double"))
      .withColumn("user_score_num", F.col("user_score").cast("double"))
)

# COMMAND ----------

# MAGIC %md ## 4. Acurácia — faixas de valores esperadas

# COMMAND ----------

df.select(
    F.min("meta_score_num").alias("meta_score_min"), F.max("meta_score_num").alias("meta_score_max"),
).show()
df.select(
    F.min("user_score_num").alias("user_score_min"),
    F.max("user_score_num").alias("user_score_max"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Achado:** `meta_score` varia de 37 a 99 (dentro da escala válida 0-100) e `user_score` fica
# MAGIC dentro de 0-10. Nenhum valor fora do domínio esperado foi encontrado — não foi necessário
# MAGIC tratamento de acurácia nessas colunas.

# COMMAND ----------

# MAGIC %md ## 5. Outliers

# COMMAND ----------

from pyspark.sql import functions as F
q1, q3 = df.approxQuantile("meta_score_num", [0.25, 0.75], 0.01)
iqr = q3 - q1
lim_inf, lim_sup = q1 - 1.5 * iqr, q3 + 1.5 * iqr
outliers = df.filter((F.col("meta_score_num") < lim_inf) | (F.col("meta_score_num") > lim_sup))
print(f"Limites IQR para meta_score: [{lim_inf:.1f}, {lim_sup:.1f}] | Outliers encontrados: {outliers.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Achado:** aplicando a regra do IQR (1,5x) sobre `meta_score`, não há outliers relevantes —
# MAGIC a distribuição de notas de crítica é razoavelmente concentrada (mediana ~77, desvio padrão
# MAGIC ~10,6). Isso é esperado: o Metacritic já agrega várias avaliações antes de publicar a nota,
# MAGIC o que naturalmente suaviza extremos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo executivo da qualidade de dados
# MAGIC | Dimensão | Problema encontrado | Tratamento |
# MAGIC |---|---|---|
# MAGIC | Completude | 35,2% nulos em meta_score; 21,7% em user_score; 11,2% em esrb_rating | Mantidos como NULL / rotulados "Nao informado"; nunca imputados |
# MAGIC | Unicidade | 2 duplicatas (title+platform) | Removidas via dropDuplicates |
# MAGIC | Consistência | 1 valor inválido de plataforma (`TG16)`); 30 datas não-padrão (TBA/Canceled) | Registro inválido descartado; status de lançamento separado da data |
# MAGIC | Acurácia | Nenhum valor fora do domínio esperado | N/A |
# MAGIC | Outliers | Nenhum outlier relevante em meta_score (regra IQR) | N/A |
