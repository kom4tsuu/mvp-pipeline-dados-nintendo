# Catálogo de Dados — MVP Nintendo Games

Catálogo referente ao modelo dimensional da camada **Gold** (`nintendo_games.gold`),
construído a partir de `silver.jogos_limpos`. Linhagem completa: **Bronze** (`bronze.jogos_raw`,
cópia fiel do `NintendoGames.csv`) → **Silver** (`silver.jogos_limpos`, dados limpos e tipados) →
**Gold** (tabelas abaixo).

---

## gold.fato_jogos
Tabela fato. **Grão:** um lançamento (um jogo em uma plataforma específica).

| Campo | Tipo | Descrição | Domínio / Linhagem |
|---|---|---|---|
| jogo_id | bigint | Identificador único do lançamento (surrogate key) | Gerado via `monotonically_increasing_id()` na Gold |
| titulo_jogo | string | Nome do jogo | Veio direto de `bronze.title`, sem transformação |
| plataforma_id | int | FK para `dim_plataforma` | — |
| genero_id | int | FK para `dim_genero` (gênero primário) | Primeiro item da lista original em `genres` |
| classificacao_id | int | FK para `dim_classificacao_etaria` | — |
| data_id | int | FK para `dim_data` (formato yyyyMMdd); `NULL` se não lançado | — |
| meta_score | double | Nota agregada da crítica | 0 a 100; `NULL` = sem nota suficiente na fonte |
| user_score | double | Nota agregada dos usuários | 0.0 a 10.0; `NULL` = sem nota suficiente na fonte |
| release_status | string | Situação do lançamento | Valores: `Lancado`, `A anunciar`, `Cancelado` — derivado de `bronze.date` na Silver |
| release_year | int | Ano de lançamento (quando aplicável) | Extraído de `release_date` |
| link | string | Caminho relativo da página do jogo no Metacritic | Veio direto de `bronze.link` |

## gold.dim_plataforma
| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| plataforma_id | int | Chave surrogate | — |
| nome_plataforma | string | Nome da plataforma Nintendo | 3DS, Switch, DS, WII, WIIU, GBA, GC, N64, iOS |

## gold.dim_genero
| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| genero_id | int | Chave surrogate | — |
| nome_genero | string | Gênero primário do jogo | Ex.: Action, Role-Playing, Strategy, Puzzle, Sports (118 valores possíveis na fonte original considerando subgêneros) |

## gold.dim_classificacao_etaria
| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| classificacao_id | int | Chave surrogate | — |
| sigla_esrb | string | Sigla da classificação ESRB | E, E10+, T, M, RP, Nao informado |
| descricao | string | Descrição por extenso da sigla | Mapeamento fixo definido na Gold |

## gold.dim_data
Granularidade: dia de lançamento.

| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| data_id | int | Chave surrogate (yyyyMMdd) | — |
| data_completa | date | Data completa do lançamento | 2006-04-10 a 2023-11-17 (dados observados) |
| ano | int | Ano | 1996 a 2023 |
| mes | int | Mês | 1 a 12 |
| trimestre | int | Trimestre | 1 a 4 |
| nome_mes | string | Nome do mês por extenso | Janeiro...Dezembro |

## gold.dim_desenvolvedora
| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| desenvolvedora_id | int | Chave surrogate | — |
| nome_desenvolvedora | string | Nome do estúdio desenvolvedor | 207 valores distintos observados (ex.: Nintendo, Intelligent Systems, Retro Studios) |

## gold.ponte_jogo_genero
Tabela-ponte (relação N:N — um jogo pode ter múltiplos gêneros/subgêneros).

| Campo | Tipo | Descrição |
|---|---|---|
| jogo_id | bigint | FK para `fato_jogos` |
| genero_id | int | FK para `dim_genero` |

## gold.ponte_jogo_desenvolvedora
Tabela-ponte (relação N:N — um jogo pode ter mais de uma desenvolvedora envolvida).

| Campo | Tipo | Descrição |
|---|---|---|
| jogo_id | bigint | FK para `fato_jogos` |
| desenvolvedora_id | int | FK para `dim_desenvolvedora` |

---

## Linhagem resumida (todas as tabelas)
Todas as tabelas Gold derivam de `silver.jogos_limpos`, que por sua vez deriva de
`bronze.jogos_raw` (cópia 1:1 do `NintendoGames.csv`, fonte: dataset "Nintendo Games" —
dados de metacritic.com, licença CC0: Public Domain). Nenhuma tabela Gold recebe dados de
fontes externas ao arquivo original.
