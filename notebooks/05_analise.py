# Databricks notebook source
# MAGIC %md
# MAGIC # Análise Final — Respondendo às Perguntas de Negócio
# MAGIC **MVP: Pipeline de Dados na Nuvem — Nintendo Games**
# MAGIC
# MAGIC Problema: **Quais fatores (plataforma, gênero, desenvolvedora, classificação etária, ano de
# MAGIC lançamento) mais influenciam a avaliação crítica e de usuários dos jogos da Nintendo, e como
# MAGIC o catálogo de jogos evoluiu ao longo do tempo?**
# MAGIC
# MAGIC Os números abaixo já foram validados sobre o dataset completo (1094 registros) e devem bater
# MAGIC com o que você obtiver rodando estas queries na camada Gold.

# COMMAND ----------

CATALOG = "nintendo_games"
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1 — Qual plataforma tem, em média, os jogos mais bem avaliados pela crítica?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.nome_plataforma,
# MAGIC        COUNT(*) AS qtd_jogos,
# MAGIC        ROUND(AVG(f.meta_score), 2) AS media_meta_score,
# MAGIC        ROUND(AVG(f.user_score), 2) AS media_user_score
# MAGIC FROM nintendo_games.gold.fato_jogos f
# MAGIC JOIN nintendo_games.gold.dim_plataforma p ON f.plataforma_id = p.plataforma_id
# MAGIC GROUP BY p.nome_plataforma
# MAGIC ORDER BY media_meta_score DESC

# MAGIC %md
# MAGIC **Resposta:** o **N64** lidera com nota média de crítica **83,97** (31 jogos), seguido por
# MAGIC **GBA (79,0)**, **Switch (78,0)** e **GameCube (77,9)**. As plataformas mais recentes e com
# MAGIC catálogo maior — **3DS (73,5)** e **WII (73,5)** — ficam nas posições mais baixas. Isso sugere
# MAGIC que catálogos menores e mais curados (N64, GBA, GC) tendem a ter nota média mais alta do que
# MAGIC catálogos grandes com jogos de nicho ou menor orçamento (3DS, WII, com centenas de títulos).
# MAGIC **iOS** tem a pior média (67,4), mas com base pequena (14 jogos).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2 — Existe correlação entre a nota da crítica e a nota dos usuários?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT corr(meta_score, user_score * 10) AS correlacao_meta_user
# MAGIC FROM nintendo_games.gold.fato_jogos
# MAGIC WHERE meta_score IS NOT NULL AND user_score IS NOT NULL

# MAGIC %md
# MAGIC **Resposta:** correlação de **0,625** (n=690 jogos com ambas as notas) — moderada a forte e
# MAGIC positiva. Crítica e usuários tendem a concordar na direção geral (jogo bem avaliado por um
# MAGIC lado tende a ser bem avaliado pelo outro), mas a correlação não é perto de 1, então há espaço
# MAGIC real de divergência: existem jogos "queridinhos da crítica" que usuários avaliam pior, e vice-versa.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 3 — Quais gêneros concentram as maiores notas médias de crítica?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT g.nome_genero,
# MAGIC        COUNT(*) AS qtd_jogos,
# MAGIC        ROUND(AVG(f.meta_score), 2) AS media_meta_score
# MAGIC FROM nintendo_games.gold.fato_jogos f
# MAGIC JOIN nintendo_games.gold.dim_genero g ON f.genero_id = g.genero_id
# MAGIC GROUP BY g.nome_genero
# MAGIC HAVING COUNT(*) >= 15
# MAGIC ORDER BY media_meta_score DESC

# MAGIC %md
# MAGIC **Resposta:** **Strategy** (82,1, 91 jogos) e **Action Adventure** (81,3, 63 jogos) têm as
# MAGIC melhores médias entre gêneros com volume relevante (≥15 jogos). **Role-Playing** também se
# MAGIC destaca (77,9, 139 jogos — o segundo maior volume). No outro extremo, **Adventure** (68,9) e
# MAGIC **Miscellaneous** (72,5, mas com 234 jogos — o maior volume do catálogo) puxam a média para
# MAGIC baixo, consistente com "Miscellaneous" normalmente agrupar jogos de menor orçamento/minigames.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 4 — Como evoluíram lançamentos e nota média de crítica por ano?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT d.ano,
# MAGIC        COUNT(*) AS qtd_lancamentos,
# MAGIC        ROUND(AVG(f.meta_score), 2) AS media_meta_score
# MAGIC FROM nintendo_games.gold.fato_jogos f
# MAGIC JOIN nintendo_games.gold.dim_data d ON f.data_id = d.data_id
# MAGIC WHERE d.ano <= 2023
# MAGIC GROUP BY d.ano
# MAGIC ORDER BY d.ano

# MAGIC %md
# MAGIC **Resposta:** o volume de lançamentos cresce fortemente até um pico em **2007 (86)** e
# MAGIC **2009 (90)** — era Wii/DS — depois oscila entre 40-70 lançamentos/ano na década seguinte.
# MAGIC A nota média de crítica, por outro lado, é **mais alta nos anos iniciais** (1996-2002, quase
# MAGIC sempre acima de 80) e **cai e se estabiliza em torno de 73-78** a partir de 2004, quando o
# MAGIC volume de lançamentos explode. Isso é coerente com a Pergunta 1: mais jogos no catálogo
# MAGIC tende a puxar a média para baixo (mais variedade de orçamento e qualidade).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 5 — Quais desenvolvedoras (fora a própria Nintendo) têm os jogos mais bem avaliados?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dv.nome_desenvolvedora,
# MAGIC        COUNT(*) AS qtd_jogos,
# MAGIC        ROUND(AVG(f.meta_score), 2) AS media_meta_score
# MAGIC FROM nintendo_games.gold.ponte_jogo_desenvolvedora pd
# MAGIC JOIN nintendo_games.gold.dim_desenvolvedora dv ON pd.desenvolvedora_id = dv.desenvolvedora_id
# MAGIC JOIN nintendo_games.gold.fato_jogos f ON pd.jogo_id = f.jogo_id
# MAGIC WHERE dv.nome_desenvolvedora != 'Nintendo'
# MAGIC GROUP BY dv.nome_desenvolvedora
# MAGIC HAVING COUNT(*) >= 8
# MAGIC ORDER BY media_meta_score DESC
# MAGIC LIMIT 15

# MAGIC %md
# MAGIC **Resposta:** **Retro Studios** lidera com média **89,4** (10 jogos — inclui a série Metroid
# MAGIC Prime), seguida por **Monolith Soft (83,7)** e **Rare Ltd. (83,7)**. **Intelligent Systems**
# MAGIC (desenvolvedora de Fire Emblem/Advance Wars) tem o maior volume entre terceiros com nota alta
# MAGIC (99 jogos, média 80,8). Isso mostra que estúdios parceiros de longa data da Nintendo entregam
# MAGIC qualidade consistente, às vezes superior à média dos jogos com selo "Nintendo" no desenvolvimento.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 6 — A classificação etária (ESRB) influencia a nota média?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ce.sigla_esrb, ce.descricao,
# MAGIC        COUNT(*) AS qtd_jogos,
# MAGIC        ROUND(AVG(f.meta_score), 2) AS media_meta_score,
# MAGIC        ROUND(AVG(f.user_score), 2) AS media_user_score
# MAGIC FROM nintendo_games.gold.fato_jogos f
# MAGIC JOIN nintendo_games.gold.dim_classificacao_etaria ce ON f.classificacao_id = ce.classificacao_id
# MAGIC GROUP BY ce.sigla_esrb, ce.descricao
# MAGIC ORDER BY media_meta_score DESC

# MAGIC %md
# MAGIC **Resposta:** jogos classificados **T (Adolescentes)** têm a maior média (79,5), seguidos por
# MAGIC **M (Maduro, 78,6 — mas apenas 15 jogos)**, **E10+ (76,3)** e **E (Livre, 75,7 — a maior
# MAGIC categoria, com 660 jogos)**. A diferença entre categorias é pequena (75,7 a 79,5 pontos), então
# MAGIC classificação etária **não é um fator determinante** de qualidade — jogos "E" (a maioria do
# MAGIC catálogo Nintendo) têm nota só um pouco abaixo dos "T", sem uma tendência forte.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussão geral
# MAGIC O catálogo de jogos da Nintendo mostra que **volume e nota média têm relação inversa**: quanto
# MAGIC mais uma plataforma, gênero ou período concentra lançamentos, mais a média se aproxima do
# MAGIC centro (73-78), enquanto nichos menores e mais curados (N64, Strategy, Retro Studios) sustentam
# MAGIC médias mais altas. A correlação moderada (0,63) entre crítica e usuários indica que ambas as
# MAGIC métricas são relevantes e não substitutas uma da outra para avaliar um jogo. Classificação
# MAGIC etária, isoladamente, não parece ser um preditor forte de qualidade percebida.
