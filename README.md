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

A seguir são descritas as etapas para efetuar a carga dos dados:

**Etapa 1 - Configuração do catálogo e schemas**

Antes de qualquer ingestão, o notebook garante que o catálogo `nintendo_games` e os schemas `bronze`, `silver` e `gold` existam no Unity Catalog. Essa etapa é o que dá organização ao Lakehouse desde o início: cada camada da Arquitetura Medalhão vive isolada no seu próprio schema, o que evita confusão entre dado bruto e dado tratado e facilita aplicar permissões de acesso diferentes para cada camada no futuro.

**Etapa 2 - Leitura do CSV bruto**

O arquivo é lido diretamente do Volume com `spark.read.csv`, com todas as colunas tratadas como texto. Essa decisão é intencional: o objetivo da camada Bronze não é interpretar ou corrigir o dado, e sim trazê-lo para dentro do ambiente de nuvem exatamente como ele chegou da fonte original. Qualquer tipagem ou correção fica reservada para a etapa seguinte do pipeline (Silver), evitando que um erro de conversão precoce esconda um problema real do dado de origem.

**Etapa 3 - Adição de metadados de controle (linhagem/rastreabilidade)**

Colunas como `_ingestion_timestamp` (quando o dado entrou no pipeline) e `_source_file`/`_source_origin` (de onde ele veio) são adicionadas ao dado bruto. Essa etapa é o que garante rastreabilidade: em um
cenário real, com múltiplas cargas ao longo do tempo, essas colunas permitem responder perguntas como "quando esse registro entrou no sistema?" e "de qual arquivo/fonte ele veio?", sem depender da memória de quem construiu o pipeline.

**Etapa 4 - Persistência como tabela Delta (Bronze)**

Por fim, o DataFrame é gravado como tabela Delta (`bronze.jogos_raw`) dentro do Unity Catalog. Usar o formato Delta em vez de simplesmente manter o CSV como arquivo é o que transforma o armazenamento bruto em Lakehouse de verdade: passa a existir controle transacional, histórico de versões (time travel) e a possibilidade de consultar o dado com SQL diretamente, preparando o terreno para as transformações da camada Silver.

Tabela Delta (Bronze)

|meta_score|title|platform|date|user_score|link|esrb_rating|developers|genres|_ingestion_timestamp|_source_file|_source_origin|
|---|---|---|---|---|---|---|---|---|---|---|---|
|null|Super Mario RPG|Switch|Nov 17, 2023|null|/game/switch/super-mario-rpg|E|['Nintendo']|['Role-Playing', 'Japanese-Style']|2026-09-16T22:52:35.195+00:00|NintendoGames.csv|Kaggle - Nintendo Games Dataset (scraped from metacritic.com), licenca CC0: Public Domain|
|null|WarioWare: Move It!|Switch|Nov 3, 2023|null|/game/switch/warioware-move-it!|RP|['Intelligent Systems']|['Miscellaneous', 'Party / Minigame']|2026-09-16T22:52:35.195+00:00|NintendoGames.csv|Kaggle - Nintendo Games Dataset (scraped from metacritic.com), licenca CC0: Public Domain|
|null|Super Mario Bros. Wonder|Switch|Oct 20, 2023|null|/game/switch/super-mario-bros-wonder|E|['Nintendo']|['Action', 'Platformer', '2D']|2026-09-16T22:52:35.195+00:00|NintendoGames.csv|Kaggle - Nintendo Games Dataset (scraped from metacritic.com), licenca CC0: Public Domain|
|null|Detective Pikachu Returns|Switch|Oct 6, 2023|null|/game/switch/detective-pikachu-returns|null|['Creatures Inc.']|['Adventure', '3D', 'Third-Person']|2026-09-16T22:52:35.195+00:00|NintendoGames.csv|Kaggle - Nintendo Games Dataset (scraped from metacritic.com), licenca CC0: Public Domain|
|null|Fae Farm|Switch|Sep 8, 2023|null|/game/switch/fae-farm|E10+|['Phoenix Labs']|['Simulation', 'Virtual', 'Virtual Life']|2026-09-16T22:52:35.195+00:00|NintendoGames.csv|Kaggle - Nintendo Games Dataset (scraped from metacritic.com), licenca CC0: Public Domain|
|87|Pikmin 4|Switch|Jul 21, 2023|9.0|/game/switch/pikmin-4|E10+|['Nintendo']|['Strategy', 'Real-Time', 'General']|2026-09-16T22:52:35.195+00:00|NintendoGames.csv|Kaggle - Nintendo Games Dataset (scraped from metacritic.com), licenca CC0: Public Domain|
|null|Pokemon Sleep|iOS|Jul 20, 2023|null|/game/ios/pokemon-sleep|null|['The Pokemon Company', ' Select Button']|['Role-Playing', 'Miscellaneous', 'Application', 'Trainer']|2026-09-16T22:52:35.195+00:00|NintendoGames.csv|Kaggle - Nintendo Games Dataset (scraped from metacritic.com), licenca CC0: Public Domain|
|74|Mario Kart 8 Deluxe: Booster Course Pass - Wave 5|Switch|Jul 12, 2023|7.6|/game/switch/mario-kart-8-deluxe-booster-course-pass---wave-5|null|['Nintendo']|['Racing', 'Arcade', 'Automobile']|2026-09-16T22:52:35.195+00:00|NintendoGames.csv|Kaggle - Nintendo Games Dataset (scraped from metacritic.com), licenca CC0: Public Domain|
|56|Everybody 1-2-Switch!|Switch|Jun 30, 2023|5.4|/game/switch/everybody-1-2-switch!|E|['Nintendo']|['Miscellaneous', 'Party / Minigame']|2026-09-16T22:52:35.195+00:00|NintendoGames.csv|Kaggle - Nintendo Games Dataset (scraped from metacritic.com), licenca CC0: Public Domain|
|82|Pikmin 1|Switch|Jun 21, 2023|8.4|/game/switch/pikmin-1|E10+|['Nintendo']|['Strategy', 'Real-Time', 'General']|2026-09-16T22:52:35.195+00:00|NintendoGames.csv|Kaggle - Nintendo Games Dataset (scraped from metacritic.com), licenca CC0: Public Domain|

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

<img width="218" height="282" alt="image" src="https://github.com/user-attachments/assets/550a8277-94dc-4fca-9b44-c0ebc8b72428" />

Achado: o valor TG16) aparece 1 vez e não corresponde a nenhuma plataforma Nintendo válida (resíduo de parsing da fonte original). Tratamento: registro descartado na Silver, com a decisão documentada (não é seguro inferir a plataforma correta a partir de 1 registro).

<img width="373" height="208" alt="image" src="https://github.com/user-attachments/assets/427b9b50-bb43-4fcc-bd78-55fbba0cf791" />

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

**1. Qual plataforma Nintendo possui, em média, os jogos mais bem avaliados pela crítica?**

O N64 tem a melhor nota média de crítica da história da Nintendo: 83,97 pontos, em um catálogo pequeno de 31 jogos. Logo atrás vêm Game Boy Advance (79,0), Switch (78,0) e GameCube (77,9). Já 3DS e Wii, os catálogos maiores (centenas de jogos cada), ficam com médias mais baixas, em torno de 73,5. O padrão se repete em outras perguntas: quanto maior e mais variado é o catálogo de uma plataforma, mais a média se aproxima de um valor intermediário. O iOS teve a pior média (67,4), mas com apenas 14 jogos — amostra pequena demais para uma conclusão definitiva.

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

**2. Existe correlação entre a nota da crítica (`meta_score`) e a nota dos usuários (`user_score`)?**

Na maior parte das vezes, sim, mas não sempre. Entre os 690 jogos que têm as duas notas, o nível de concordância entre crítica e usuários é de 0,625 em uma escala de 0 a 1 (quanto mais perto de 1, mais as duas notas andam juntas) — uma concordância forte, mas longe de ser total. Ou seja, um jogo bem avaliado pela crítica tem boas chances de agradar também os jogadores, mas existe um número relevante de exceções nos dois sentidos. As duas notas se complementam; nenhuma substitui a outra.

|correlacao_meta_user|
|---|
|0.625559270411665|

**3. Quais gêneros de jogos concentram as maiores notas médias de crítica?**

Entre os gêneros com pelo menos 15 jogos lançados (para não deixar um único sucesso distorcer a média), os melhor avaliados são Strategy (82,1 em 91 jogos) e Action Adventure (81,3 em 63 jogos). Os RPGs (Role-Playing) vêm na sequência, com 77,9 de média em 139 jogos — o maior catálogo entre os gêneros bem avaliados. No outro extremo, Adventure (68,9) tem a pior média entre os gêneros relevantes, e Miscellaneous (72,5) — a categoria mais volumosa do catálogo, com 234 jogos — também fica abaixo da média geral.

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

**4. Como evoluíram o número de lançamentos e a nota média de crítica ao longo dos anos?**

Nos primeiros anos do dataset (1996 a 2002), a Nintendo lançava poucos jogos por ano, mas praticamente todos com nota acima de 80 pontos. A partir de meados dos anos 2000, o volume de lançamentos cresceu de forma acentuada — com picos em 2007 (86 jogos) e 2009 (90 jogos) —, e a nota média de crítica caiu e se estabilizou entre 73 e 78 pontos, patamar que se mantém até hoje. O dado não indica queda de qualidade da Nintendo como empresa; indica que, ao produzir mais jogos para públicos mais variados, a média do catálogo naturalmente se aproxima do centro — o mesmo efeito observado na Pergunta 1.

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

**5. Quais desenvolvedoras (fora a própria Nintendo) produzem os jogos mais bem avaliados?**

Excluindo a própria Nintendo, a Retro Studios lidera com média de 89,4 pontos (10 jogos, incluindo a série Metroid Prime). Em seguida, empatadas, Monolith Soft e Rare Ltd., ambas com 83,7. Destaque para a Intelligent Systems (Fire Emblem, Advance Wars): além de manter nota alta (80,8), é a desenvolvedora terceira com maior volume no catálogo — 99 jogos —, mostrando que consegue qualidade consistente mesmo em grande escala.

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

**6. A classificação etária (ESRB) influencia a nota média dos jogos?**

Não. As notas médias por classificação etária ficam dentro de uma faixa estreita: jogos T (Adolescentes) têm a maior média (79,5), seguidos por M (Maduro, 78,6) — base pequena, só 15 jogos —, E10+ (76,3) e E (Livre para todos, 75,7). A categoria E, de longe a mais comum no catálogo (660 dos 1.094 jogos, ou 6 em cada 10 lançamentos), fica apenas cerca de 4 pontos abaixo da líder. Ou seja, a classificação etária não é um fator determinante de qualidade no catálogo Nintendo.

|sigla_esrb|descricao|qtd_jogos|media_meta_score|media_user_score|
|---|---|---|---|---|
|T|Adolescentes (13+)|150|79.49|8.07|
|M|Maduro (17+)|15|78.57|8.08|
|E10+|Livre para maiores de 10 anos|142|76.31|7.76|
|E|Livre para todos|657|75.67|7.63|
|Nao informado|Nao informado pela fonte|122|71.29|7.17|
|RP|Classificacao pendente|5|null|8|

**Conclusão geral**

O padrão que se repete nas seis respostas é o mesmo: volume e nota média caminham em direções opostas. Recortes menores e mais concentrados (N64, Strategy, Retro Studios, os primeiros anos da empresa) sustentam médias mais altas; recortes grandes e diversos (3DS, Wii, Miscellaneous, os anos de pico de lançamentos) tendem a ficar mais perto da média geral do catálogo. A classificação etária é a exceção: não segue esse padrão e não se mostrou um fator relevante de qualidade.

---

## 7. Autoavaliação

Desenvolver este MVP foi uma experiência enriquecedora e, ao mesmo tempo, bastante desafiadora. Consegui atingir os objetivos que tracei no início do trabalho: construí o pipeline de ponta a ponta, passando pelas cinco etapas propostas (definição do objetivo, coleta, modelagem, carga/ETL e análise), e cheguei ao final com as seis perguntas de negócio respondidas com dados reais extraídos do próprio pipeline. Mais do que isso, senti que consegui aplicar na prática boa parte dos conceitos vistos em aula — Arquitetura Medalhão, Lakehouse, Esquema Estrela, ETL — que até então eram só teoria para mim.

A maior dificuldade não foi entender os conceitos, mas lidar com problemas reais de dados durante a implementação, que é exatamente o tipo de imprevisto que o enunciado avisa que vai acontecer no dia a dia de um Engenheiro de Dados. Tive três erros na camada Silver e na camada de Qualidade de Dados que me obrigaram a rever minhas transformações: um valor de data fora do padrão esperado ("Q4 2015") que quebrou a conversão de datas porque minha lógica inicial só previa os casos "TBA" e "Canceled"; uma célula de notebook mal separada, misturando código Python com texto explicativo, que gerava erro de sintaxe; e uma tentativa de calcular estatísticas (quartis, outliers) diretamente sobre uma coluna que a camada Bronze mantém propositalmente como texto, sem conversão numérica. Resolver cada um desses problemas me fez entender na prática por que a documentação e o tratamento de exceções são tão importantes num pipeline de dados — no fim das contas, quase nenhum dataset do mundo real vem "limpo" como a gente espera.

Fiquei bastante satisfeito com o resultado final, principalmente por ter trabalhado com um tema que gosto de verdade, jogos eletrônicos. Isso deixou o processo de formular as perguntas de negócio e interpretar os resultados muito mais natural, porque eu já tinha familiaridade com o contexto (plataformas, desenvolvedoras, gêneros) e conseguia perceber quando um resultado fazia sentido ou merecia um olhar mais atento.

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


