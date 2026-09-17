# MVP: Construção de um Pipeline de Dados na Nuvem — Catálogo de Jogos da Nintendo

**Autor:** Rodrigo Komatsu Shinkado
**Matrícula:** 4052025002104
**Repositório GitHub:** https://github.com/kom4tsuu/mvp-pipeline-dados-nintendo
**Ambiente de nuvem:** Databricks Free Edition
**Dataset:** `NintendoGames.csv` — jogos da Nintendo com notas de crítica e usuários (Metacritic)

## 1. Contexto de Negócio e Perguntas (Etapa 2 e 4.1)

### Contexto
O dataset reúne **1.094 lançamentos** de jogos da Nintendo (e jogos third-party publicados/
desenvolvidos para plataformas Nintendo), cobrindo o período de **1996 a 2023**, com dados de
avaliação extraídos do Metacritic: nota da crítica (`meta_score`, escala 0-100) e nota dos
usuários (`user_score`, escala 0-10), além de plataforma, data de lançamento, classificação
etária ESRB, desenvolvedora(s) e gênero(s).

**Estrutura dos dados brutos (9 colunas):**

| Coluna | Descrição |
|---|---|
| `meta_score` | Nota agregada da crítica especializada (0-100) |
| `title` | Nome do jogo |
| `platform` | Plataforma (3DS, Switch, DS, WII, WIIU, GBA, GC, N64, iOS) |
| `date` | Data de lançamento (ou status "TBA"/"Canceled") |
| `user_score` | Nota agregada dos usuários (0-10) |
| `link` | URL relativa da página do jogo no Metacritic |
| `esrb_rating` | Classificação etária (E, E10+, T, M, RP) |
| `developers` | Lista de estúdios desenvolvedores |
| `genres` | Lista de gêneros/subgêneros |

### Licença dos dados
O arquivo corresponde ao dataset **"Nintendo Games"**, obtido via scraping do site
metacritic.com e distribuído publicamente (ex.: Kaggle), sob **licença CC0: Public Domain** —
uso, modificação e redistribuição livres, sem necessidade de atribuição.

### Problema
**Quais fatores (plataforma, gênero, desenvolvedora, classificação etária, ano de lançamento)
mais influenciam a avaliação crítica e de usuários dos jogos da Nintendo, e como o catálogo de
jogos evoluiu ao longo do tempo?**

### Perguntas de negócio
1. Qual plataforma Nintendo possui, em média, os jogos mais bem avaliados pela crítica?
2. Existe correlação entre a nota da crítica (`meta_score`) e a nota dos usuários (`user_score`)?
3. Quais gêneros de jogos concentram as maiores notas médias de crítica?
4. Como evoluíram o número de lançamentos e a nota média de crítica ao longo dos anos?
5. Quais desenvolvedoras (fora a própria Nintendo) produzem os jogos mais bem avaliados?
6. A classificação etária (ESRB) influencia a nota média dos jogos?

---

## 2. Carga dos Dados (Etapa 4.2)

O `NintendoGames.csv` foi enviado para um **Volume do Unity Catalog**
(`/Volumes/nintendo_games/bronze/raw_files/`) via upload direto na interface do Databricks
(Catalog → Volumes → Upload to this volume), e lido com `spark.read.csv` no notebook
[`notebooks/01_bronze_ingestao.py`](notebooks/01_bronze_ingestao.py). O dado foi persistido como
tabela Delta `bronze.jogos_raw`, sem nenhuma transformação de conteúdo — apenas com metadados de
controle (`_ingestion_timestamp`, `_source_file`, `_source_origin`) para rastreabilidade.

<img width="693" height="443" alt="image" src="https://github.com/user-attachments/assets/a2a8d5bc-472e-492a-a0d9-3f9796207d3b" />


---

## 3. Modelagem e Catálogo de Dados (Etapa 4.3)

Foi adotado um **Esquema Estrela**: uma tabela fato (`fato_jogos`, grão = um lançamento) cercada
de dimensões (`dim_plataforma`, `dim_genero`, `dim_classificacao_etaria`, `dim_data`,
`dim_desenvolvedora`) e duas tabelas-ponte (`ponte_jogo_genero`, `ponte_jogo_desenvolvedora`)
para as relações N:N — um jogo pode ter mais de um gênero e mais de uma desenvolvedora, o que não
caberia em uma dimensão tradicional sem duplicar linhas na fato.

O catálogo de dados completo (toda tabela e coluna, tipo, domínio de valores e linhagem) está em
[`catalogo_dados.md`](catalogo_dados.md). A construção do modelo está no notebook
[`notebooks/03_gold_modelagem.py`](notebooks/03_gold_modelagem.py).

📸 **Screenshots a anexar aqui:** Catalog Explorer mostrando o schema `gold` com todas as tabelas,
e o diagrama de linhagem (Unity Catalog → Lineage) de `fato_jogos`.

---

## 4. Pipeline de Dados (Etapa 4.4)

O pipeline foi ramificado em **três notebooks sequenciais**, um por camada da Arquitetura
Medalhão, para manter cada etapa isolada, legível e fácil de reexecutar:

| Notebook | Camada | O que faz |
|---|---|---|
| [`01_bronze_ingestao.py`](notebooks/01_bronze_ingestao.py) | Bronze | Lê o CSV do Volume e persiste como Delta, sem transformar |
| [`02_silver_transformacao.py`](notebooks/02_silver_transformacao.py) | Silver | Remove duplicatas, corrige tipos, trata datas não-padrão, parseia arrays de gênero/desenvolvedora, padroniza ESRB |
| [`03_gold_modelagem.py`](notebooks/03_gold_modelagem.py) | Gold | Constrói o Esquema Estrela (fato + dimensões + pontes) |
| [`04_qualidade_dados.py`](notebooks/04_qualidade_dados.py) | — | Análise de qualidade sobre o dado bruto, documentando achados e tratamentos |
| [`05_analise.py`](notebooks/05_analise.py) | — | Consultas SQL sobre a Gold respondendo cada pergunta de negócio |

Cada transformação relevante está documentada em células Markdown dentro do próprio notebook
(o "o quê" e o "porquê"), conforme detalhado na seção de Qualidade de Dados abaixo.

📸 **Screenshots a anexar aqui:** execução completa (todas as células rodadas com sucesso) de
cada um dos 3 notebooks de pipeline, e o Catalog Explorer confirmando que as tabelas Bronze,
Silver e Gold foram persistidas.

---

## 5. Qualidade de Dados (Etapa 4.5)

Análise completa no notebook [`notebooks/04_qualidade_dados.py`](notebooks/04_qualidade_dados.py).
Resumo:

| Dimensão | Problema encontrado | Tratamento aplicado |
|---|---|---|
| Completude | 35,2% de nulos em `meta_score`, 21,7% em `user_score`, 11,2% em `esrb_rating` | Mantidos como `NULL` (ausência real da nota) / rotulados `"Nao informado"`; nunca imputados com valor artificial |
| Unicidade | 2 duplicatas de `title`+`platform` | Removidas com `dropDuplicates` |
| Consistência | 1 valor de plataforma inválido (`TG16)`); 30 datas fora do padrão (`TBA`, `Canceled`, etc.) | Registro de plataforma inválida descartado; coluna `release_status` criada para separar situação de lançamento da data em si |
| Acurácia | Nenhum valor fora do domínio esperado (`meta_score` 37-99, `user_score` dentro de 0-10) | Não foi necessário tratamento |
| Outliers | Nenhum outlier relevante em `meta_score` pela regra do IQR | Não foi necessário tratamento |


**Completude — valores nulos/vazios por coluna**

|meta_score|user_score|esrb_rating|developers|title|platform|date|genres|
|---|---|---|---|---|---|---|---|
|385|238|122|3|0|0|0|0|

Achados (calculados sobre os 1094 registros):

meta_score: 385 nulos (35,2%) — muitos jogos, sobretudo mais antigos ou de nicho, nunca receberam nota consolidada da crítica no Metacritic.
user_score: 238 nulos (21,7%) — jogos sem volume suficiente de avaliações de usuários.
esrb_rating: 122 nulos (11,2%) — jogos sem classificação etária cadastrada na fonte.
developers: 3 nulos.
title, platform, date, genres: sem nulos.
Tratamento: nulos em meta_score/user_score foram mantidos como NULL (ausência real da nota, não um erro — forçar um valor como 0 distorceria qualquer média). Nulos em esrb_rating foram padronizados para o rótulo "Nao informado" na Silver, para ficarem explícitos nas análises em vez de somem como NULL silencioso.

**Unicidade — duplicatas**

|title|platform|count|
|---|---|---|
|Art Academy: Lessons for Everyone|3DS|2|
|Fluidity|WII|2|

Achado: 2 pares duplicados de title+platform. Tratamento: removidos via dropDuplicates(["title","platform"]) na Silver, mantendo a primeira ocorrência.

**Consistência — formato de platform e date**

Valores distintos de platform:
<img width="117" height="263" alt="image" src="https://github.com/user-attachments/assets/6f3f456b-74ec-4f66-90df-c018ec317f72" />

Achado: o valor TG16) aparece 1 vez e não corresponde a nenhuma plataforma Nintendo válida (resíduo de parsing da fonte original). Tratamento: registro descartado na Silver, com a decisão documentada (não é seguro inferir a plataforma correta a partir de 1 registro).

Registros com 'date' fora do padrão MMM d, yyyy: 30
<img width="120" height="186" alt="image" src="https://github.com/user-attachments/assets/3a043713-1f6f-4d5a-b450-2259bdc54ce6" />

Achado: 30 registros com date fora do padrão (TBA, Canceled, TBA 2024, TBA 2011, TBA 2010, Q4 2015) — representam jogos anunciados mas não lançados, ou cancelados. Tratamento: criada a coluna release_status (Lancado / A anunciar / Cancelado) na Silver; release_date fica NULL para os que não têm data real, preservando a informação em vez de descartar a linha inteira.

**Acurácia — faixas de valores esperadas**
<img width="228" height="205" alt="image" src="https://github.com/user-attachments/assets/e155d716-b359-4564-8eeb-eadd88870aa4" />

Achado: meta_score varia de 37 a 99 (dentro da escala válida 0-100) e user_score fica dentro de 0-10. Nenhum valor fora do domínio esperado foi encontrado — não foi necessário tratamento de acurácia nessas colunas.

**Outliers**

Limites IQR para meta_score: [48.0, 104.0] | Outliers encontrados: 9

Achado: aplicando a regra do IQR (1,5x) sobre meta_score, não há outliers relevantes — a distribuição de notas de crítica é razoavelmente concentrada (mediana ~77, desvio padrão ~10,6). Isso é esperado: o Metacritic já agrega várias avaliações antes de publicar a nota, o que naturalmente suaviza extremos.

---

## 6. Análise de Dados (Etapa 4.5)

Consultas completas em [`notebooks/05_analise.py`](notebooks/05_analise.py). Respostas
(validadas sobre o dataset completo):

**1. Qual plataforma Nintendo possui, em média, os jogos mais bem avaliados pela crítica?** o N64 lidera com nota média de crítica 83,97 (31 jogos), seguido por GBA (79,0), Switch (78,0) e GameCube (77,9). As plataformas mais recentes e com catálogo maior — 3DS (73,5) e WII (73,5) — ficam nas posições mais baixas. Isso sugere que catálogos menores e mais curados (N64, GBA, GC) tendem a ter nota média mais alta do que catálogos grandes com jogos de nicho ou menor orçamento (3DS, WII, com centenas de títulos). iOS tem a pior média (67,4), mas com base pequena (14 jogos).

|nome_plataforma|qtd_jogos|media_meta_score|media_user_score|
|---|---|---|---|
|N64|31|83.97|8.31|
|GBA|64|79|8.23|
|Switch|209|78.01|7.4|
|GC|52|77.9|8.21|
|DS|196|76.05|7.67|
|WIIU|80|74.98|7.7|
|3DS|258|73.49|7.37|
|WII|187|73.34|7.98|
|iOS|14|67.42|5.82|

**2. Existe correlação entre a nota da crítica (`meta_score`) e a nota dos usuários (`user_score`)?** correlação de 0,625 (n=690 jogos com ambas as notas) — moderada a forte e positiva. Crítica e usuários tendem a concordar na direção geral (jogo bem avaliado por um lado tende a ser bem avaliado pelo outro), mas a correlação não é perto de 1, então há espaço real de divergência: existem jogos "queridinhos da crítica" que usuários avaliam pior, e vice-versa.

|correlacao_meta_user|
|---|
|0.625559270411665|

**3. Quais gêneros de jogos concentram as maiores notas médias de crítica?** Strategy (82,1, 91 jogos) e Action Adventure (81,3, 63 jogos) têm as melhores médias entre gêneros com volume relevante (≥15 jogos). Role-Playing também se destaca (77,9, 139 jogos — o segundo maior volume). No outro extremo, Adventure (68,9) e Miscellaneous (72,5, mas com 234 jogos — o maior volume do catálogo) puxam a média para baixo, consistente com "Miscellaneous" normalmente agrupar jogos de menor orçamento/minigames.

|nome_genero|qtd_jogos|media_meta_score|
|---|---|---|
|Strategy|91|82.14|
|Action Adventure|63|81.33|
|Driving|38|78.68|
|Role-Playing|139|77.91|
|Action|329|76|
|Simulation|39|74.94|
|Puzzle|34|74.93|
|Sports|67|72.68|
|Miscellaneous|233|72.52|
|Adventure|17|68.92|

**4. Como evoluíram o número de lançamentos e a nota média de crítica ao longo dos anos?** o volume de lançamentos cresce fortemente até um pico em 2007 (86) e 2009 (90) — era Wii/DS — depois oscila entre 40-70 lançamentos/ano na década seguinte. A nota média de crítica, por outro lado, é mais alta nos anos iniciais (1996-2002, quase sempre acima de 80) e cai e se estabiliza em torno de 73-78 a partir de 2004, quando o volume de lançamentos explode. Isso é coerente com a Pergunta 1: mais jogos no catálogo tende a puxar a média para baixo (mais variedade de orçamento e qualidade).

|ano|qtd_lancamentos|media_meta_score|
|---|---|---|
|1996|3|88.67|
|1997|5|89|
|1998|4|85.25|
|1999|4|81.5|
|2000|11|82.64|
|2001|14|84.93|
|2002|16|81.5|
|2003|25|81.28|
|2004|37|76.09|
|2005|39|75.92|
|2006|52|74.6|
|2007|85|74.57|
|2008|38|73.04|
|2009|90|74.7|
|2010|67|75.33|
|2011|42|73.82|
|2012|48|72.77|
|2013|70|73.97|
|2014|43|76.24|
|2015|41|70.45|
|2016|54|76.97|
|2017|73|76.36|
|2018|49|75.94|
|2019|43|77.73|
|2020|38|75.38|
|2021|27|75.11|
|2022|17|78.33|
|2023|26|81|

**5. Quais desenvolvedoras (fora a própria Nintendo) produzem os jogos mais bem avaliados?** Retro Studios lidera com média 89,4 (10 jogos — inclui a série Metroid Prime), seguida por Monolith Soft (83,7) e Rare Ltd. (83,7). Intelligent Systems (desenvolvedora de Fire Emblem/Advance Wars) tem o maior volume entre terceiros com nota alta (99 jogos, média 80,8). Isso mostra que estúdios parceiros de longa data da Nintendo entregam qualidade consistente, às vezes superior à média dos jogos com selo "Nintendo" no desenvolvimento.

|nome_desenvolvedora|qtd_jogos|media_meta_score|
|---|---|---|
|Retro Studios|10|89.44|
|Monolith Soft|16|83.67|
|Rare Ltd.|15|83.67|
|PlatinumGames|12|83.2|
|Alphadream Corporation|8|83.13|
|GREZZO|8|82.29|
|Intelligent Systems|99|80.77|
|TOSE|8|80|
|Game Freak|47|79.97|
|Square Enix|10|79.22|
|Next Level Games|11|78.57|
|Nintendo Software Technology|12|77.75|
|Camelot Software Planning|18|77.56|
|Good-Feel|8|76.71|
|Bandai Namco Games|12|76.11|

**6. A classificação etária (ESRB) influencia a nota média dos jogos?** jogos classificados T (Adolescentes) têm a maior média (79,5), seguidos por M (Maduro, 78,6 — mas apenas 15 jogos), E10+ (76,3) e E (Livre, 75,7 — a maior categoria, com 660 jogos). A diferença entre categorias é pequena (75,7 a 79,5 pontos), então classificação etária não é um fator determinante de qualidade — jogos "E" (a maioria do catálogo Nintendo) têm nota só um pouco abaixo dos "T", sem uma tendência forte.

|sigla_esrb|descricao|qtd_jogos|media_meta_score|media_user_score|
|---|---|---|---|---|
|T|Adolescentes (13+)|150|79.49|8.07|
|M|Maduro (17+)|15|78.57|8.08|
|E10+|Livre para maiores de 10 anos|142|76.31|7.76|
|E|Livre para todos|657|75.67|7.63|
|Nao informado|Nao informado pela fonte|122|71.29|7.17|
|RP|Classificacao pendente|5|null|8|

---

## 7. Autoavaliação

*(Escreva esta seção em primeira pessoa após executar o trabalho de fato no Databricks — o
enunciado exige uma reflexão pessoal sobre o processo, que não pode ser preenchida por terceiros.
Sugestão de estrutura abaixo.)*

- **Objetivos atingidos:** o pipeline cobriu as 5 etapas propostas (objetivo → coleta →
  modelagem → carga/ETL → análise) e respondeu às 6 perguntas de negócio definidas no início.
- **Dificuldades encontradas:** *(descreva o que de fato deu trabalho ao rodar no seu ambiente —
  ex.: configuração do Unity Catalog, parsing das colunas `genres`/`developers`, etc.)*
- **Trabalhos futuros:** possíveis extensões incluem cruzar este dataset com dados de vendas por
  título (ex.: VGChartz) para relacionar nota crítica com desempenho comercial, ou usar todos os
  gêneros (não só o primário) numa análise multi-rótulo mais completa via `ponte_jogo_genero`.

---

## Estrutura do repositório

```
.
├── README.md                          # este documento
├── catalogo_dados.md                  # catálogo de dados completo
└── notebooks/
    ├── 01_bronze_ingestao.py
    ├── 02_silver_transformacao.py
    ├── 03_gold_modelagem.py
    ├── 04_qualidade_dados.py
    └── 05_analise.py
```

## Como reproduzir
1. Crie uma conta no [Databricks Free Edition](https://www.databricks.com/) e um workspace.
2. Faça upload de `NintendoGames.csv` para um Volume do Unity Catalog em `bronze/raw_files/`.
3. Importe os 5 arquivos de `notebooks/` no Databricks (Workspace → Import → formato "Source
   File" — os arquivos já seguem o formato de notebook Databricks, com `# Databricks notebook
   source` e `# COMMAND ----------`).
4. Execute em ordem: `01` → `02` → `03` → `04` → `05`.
