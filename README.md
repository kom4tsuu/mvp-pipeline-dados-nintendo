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
<img width="1289" height="246" alt="image" src="https://github.com/user-attachments/assets/a0ac854a-cbcd-44be-bd03-af0dee6f154b" />

**Unicidade — duplicatas**
<img width="1154" height="39" alt="image" src="https://github.com/user-attachments/assets/87ddb8e2-ab0c-4f8d-afee-9e08a3919321" />


---

## 6. Análise de Dados (Etapa 4.5)

Consultas completas em [`notebooks/05_analise.py`](notebooks/05_analise.py). Respostas
(validadas sobre o dataset completo):

**1. Plataforma com melhor nota média de crítica:** N64 (83,97), seguido de GBA (79,0), Switch
(78,0) e GameCube (77,9). 3DS e WII, os catálogos maiores, ficam nas posições mais baixas (~73,5).

**2. Correlação crítica x usuários:** 0,625 (moderada a forte, positiva, n=690) — concordância
geral, mas com espaço real de divergência entre os dois públicos.

**3. Gêneros com melhor nota (≥15 jogos):** Strategy (82,1) e Action Adventure (81,3) lideram;
Adventure (68,9) e Miscellaneous (72,5) ficam abaixo da média.

**4. Evolução por ano:** volume de lançamentos cresce até picos em 2007 (86) e 2009 (90); a nota
média de crítica é mais alta nos anos iniciais (1996-2002, quase sempre >80) e se estabiliza em
73-78 a partir de 2004, quando o volume de lançamentos aumenta.

**5. Desenvolvedoras (excl. Nintendo, ≥8 jogos):** Retro Studios lidera (89,4), seguida de
Monolith Soft e Rare Ltd. (83,7 cada). Intelligent Systems tem o maior volume entre terceiros
com nota consistentemente alta (99 jogos, média 80,8).

**6. Influência da classificação ESRB:** diferença pequena entre categorias (75,7 a 79,5) — não é
um fator determinante de qualidade.

📸 **Screenshots a anexar aqui:** resultado de cada uma das 6 queries do notebook de análise.

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
