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

**Evidências:**

**Completude — valores nulos/vazios por coluna**
<img width="1288" height="488" alt="image" src="https://github.com/user-attachments/assets/37e53145-a811-4865-abd4-c91f11bd8fdf" />

**Unicidade — duplicatas**
<img width="1176" height="292" alt="image" src="https://github.com/user-attachments/assets/0ea23ec6-c88b-4057-bd2b-8d49b49ebab5" />

**Consistência — formato de platform e date**
<img width="1331" height="408" alt="image" src="https://github.com/user-attachments/assets/1754ae43-ea97-4e82-b72a-fd22597ba42b" />

**Acurácia — faixas de valores esperadas**
<img width="1311" height="329" alt="image" src="https://github.com/user-attachments/assets/0f6b6f6e-9893-43dc-9025-758004181982" />

**Outliers**
<img width="1303" height="127" alt="image" src="https://github.com/user-attachments/assets/02ae836f-b848-4ea4-be40-51c494fb64de" />


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

**5. Desenvolvedoras (excl. Nintendo, ≥8 jogos):** Retro Studios lidera (89,4), seguida de
Monolith Soft e Rare Ltd. (83,7 cada). Intelligent Systems tem o maior volume entre terceiros
com nota consistentemente alta (99 jogos, média 80,8).

**6. Influência da classificação ESRB:** diferença pequena entre categorias (75,7 a 79,5) — não é
um fator determinante de qualidade.


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
