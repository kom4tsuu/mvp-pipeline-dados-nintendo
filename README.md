# MVP: Construção de um Pipeline de Dados na Nuvem — Catálogo de Jogos da Nintendo

**Autor:** Rodrigo Komatsu Shinkado

**Matrícula:** 4052025002104

**Repositório GitHub:** https://github.com/kom4tsuu/mvp-pipeline-dados-nintendo

**Ambiente de nuvem:** Databricks Free Edition

**Dataset:** `NintendoGames.csv` — jogos da Nintendo com notas de crítica e usuários (Metacritic)

## 1. Contexto de Negócio e Perguntas (Etapa 2 e 4.1)

### Objetivo do trabalho

A Nintendo é uma das empresas mais longevas e influentes da indústria de jogos eletrônicos,
com um catálogo que atravessa quase três décadas e múltiplas gerações de consoles — do Nintendo
64, passando por GameCube, Wii, 3DS e Wii U, até o Switch. Diferente de concorrentes cuja
estratégia gira em torno de poucas franquias de grande orçamento, a Nintendo sustenta um modelo de
negócio baseado em um catálogo extenso e diverso: jogos próprios (first-party), parcerias de longa
data com estúdios como Intelligent Systems e Retro Studios, e presença em múltiplas plataformas
com propostas de público muito diferentes entre si (desde jogos "livres para todos" até títulos
voltados a públicos mais específicos).
 
Entender o que influencia a recepção crítica e do público nesse catálogo tem valor prático real:
ajuda a identificar se certas plataformas, gêneros ou parceiros de desenvolvimento entregam
qualidade mais consistente que outros, se o crescimento do volume de lançamentos ao longo dos anos
veio acompanhado de queda ou manutenção da qualidade percebida, e até que ponto a nota da crítica
especializada reflete a opinião de quem realmente joga. São perguntas que, em um contexto real de
publisher ou estúdio, apoiariam decisões como onde concentrar investimento de desenvolvimento, quais
parcerias priorizar, ou como calibrar expectativas de qualidade para um público específico —
exatamente o tipo de necessidade de negócio que motiva um pipeline de dados como o construído
neste MVP.
 
Este trabalho tem como objetivo construir um pipeline de dados de ponta a ponta na nuvem —
passando por coleta, modelagem, carga (ETL) e análise — capaz de transformar o histórico bruto de
lançamentos da Nintendo em respostas concretas para um conjunto de perguntas de negócio definidas
a seguir, evidenciando na prática o raciocínio completo de um Engenheiro de Dados diante de um
problema real.

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
O arquivo corresponde ao dataset **["Nintendo Games"](https://www.kaggle.com/datasets/joebeachcapital/nintendo-games)**,
publicado no Kaggle pelo usuário `joebeachcapital`, contendo todos os jogos da Nintendo para
todas as plataformas, coletados via scraping do site metacritic.com. A licença informada na
página do dataset é a **[Database Contents License (DbCL) v1.0](https://opendatacommons.org/licenses/dbcl/1-0/)**,
da Open Data Commons: ela concede uma licença de copyright mundial, gratuita, não-exclusiva,
perpétua e irrevogável sobre o conteúdo da base — incluindo uso comercial, modificação e
redistribuição —, desde que respeitadas as condições da Open Database License (ODbL) que cobre a
base de dados como um todo (o que, na prática, pode incluir exigência de atribuição à fonte
original). Por esse motivo, a fonte do dataset é citada explicitamente ao longo deste documento,
em vez de tratada como dado de domínio público sem restrições.

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

<p align="center">
<img width="1362" height="768" alt="image" src="https://github.com/user-attachments/assets/fa4cbff2-ab32-45d8-9759-7e304a6afec0" />
<br>
  <sub><i>Tabela <code>bronze.jogos_raw</code> persistida no Catalog Explorer, confirmando a carga do dataset bruto na nuvem.</i></sub>
</p>

---
 
## 3. Modelagem e Catálogo de Dados (Etapa 4.3)
 
### Por que um Esquema Estrela
 
Foi adotado um **Esquema Estrela**: uma tabela fato (`fato_jogos`, grão = um lançamento) cercada
de dimensões (`dim_plataforma`, `dim_genero`, `dim_classificacao_etaria`, `dim_data`,
`dim_desenvolvedora`) e duas tabelas-ponte (`ponte_jogo_genero`, `ponte_jogo_desenvolvedora`)
para as relações N:N — um jogo pode ter mais de um gênero e mais de uma desenvolvedora, o que não
caberia em uma dimensão tradicional sem duplicar linhas na fato.
 
Essa escolha não é só uma formalidade acadêmica: é o que torna as seis perguntas de negócio da
Seção 1 possíveis de responder com uma consulta SQL simples. Sem separar plataforma, gênero,
classificação etária e data em dimensões próprias, cada pergunta ("qual plataforma tem a melhor
nota?", "qual gênero é mais bem avaliado?") exigiria reprocessar texto bruto toda vez. Com o
modelo estrela, cada pergunta vira apenas um `JOIN` entre a `fato_jogos` e a dimensão
correspondente, seguido de um `GROUP BY` — é essa estrutura que o notebook
[`notebooks/05_analise.py`](notebooks/05_analise.py) explora diretamente.
 
<p align="center">
<img width="315" height="706" alt="image" src="https://github.com/user-attachments/assets/66f27783-23c6-4c9e-8fea-732f3fc46731" />
 <br>
  <sub><i>Tabelas da camada Gold (fato + dimensões + tabelas-ponte) persistidas no Unity Catalog, compondo o Esquema Estrela do modelo dimensional.</i></sub>
</p>
A construção completa do modelo está no notebook
[`notebooks/03_gold_modelagem.py`](notebooks/03_gold_modelagem.py), que lê a tabela
`silver.jogos_limpos` já tratada e distribui seu conteúdo entre a tabela fato e as dimensões
abaixo. A seguir, cada tabela é apresentada com sua estrutura completa (esse é o Catálogo de
Dados pedido no enunciado — também disponível separadamente em
[`catalogo_dados.md`](catalogo_dados.md)): o que ela representa, cada campo, seu tipo e seu
domínio de valores.
 
**Linhagem geral:** todas as tabelas abaixo derivam de `silver.jogos_limpos`, que por sua vez
deriva de `bronze.jogos_raw` — ou seja, nenhuma informação nova é inventada na Gold, apenas
reorganizada.
 
### gold.fato_jogos — o centro do modelo
 
Esta é a tabela fato, o coração do Esquema Estrela: cada linha representa **um lançamento**, ou
seja, um jogo em uma plataforma específica (o mesmo jogo em duas plataformas diferentes gera duas
linhas). É nela que ficam as métricas que as perguntas de negócio realmente medem — `meta_score`
e `user_score` — junto com as chaves que apontam para cada dimensão.
 
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
 
### gold.dim_plataforma
 
Dimensão simples: cada linha é uma plataforma Nintendo distinta encontrada no catálogo. É ela que
responde diretamente à Pergunta 1 (qual plataforma tem a melhor nota média de crítica).
 
| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| plataforma_id | int | Chave surrogate | — |
| nome_plataforma | string | Nome da plataforma Nintendo | 3DS, Switch, DS, WII, WIIU, GBA, GC, N64, iOS |
 
### gold.dim_genero
 
Reúne os gêneros primários distintos do catálogo (o primeiro item da lista original de `genres`
de cada jogo — a lista completa, com todos os gêneros e subgêneros, fica preservada na tabela-ponte
`ponte_jogo_genero`, descrita mais abaixo). Usada diretamente na Pergunta 3.
 
| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| genero_id | int | Chave surrogate | — |
| nome_genero | string | Gênero primário do jogo | Ex.: Action, Role-Playing, Strategy, Puzzle, Sports (118 valores possíveis na fonte original considerando subgêneros) |
 
### gold.dim_classificacao_etaria
 
Traduz a sigla ESRB (como aparece no dado bruto) para uma descrição legível, e centraliza esse
"de-para" em um único lugar — em vez de repetir a explicação da sigla em cada consulta. Usada na
Pergunta 6, sobre a influência da classificação etária na nota média.
 
| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| classificacao_id | int | Chave surrogate | — |
| sigla_esrb | string | Sigla da classificação ESRB | E, E10+, T, M, RP, Nao informado |
| descricao | string | Descrição por extenso da sigla | Mapeamento fixo definido na Gold |
 
### gold.dim_data
 
Dimensão de calendário, na granularidade de dia de lançamento. Além da data completa, já vem com
ano, mês e trimestre pré-calculados — o que evita ter que extrair essas partes repetidamente em
cada consulta (é essa dimensão que sustenta a Pergunta 4, sobre a evolução do catálogo por ano).
 
| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| data_id | int | Chave surrogate (yyyyMMdd) | — |
| data_completa | date | Data completa do lançamento | 2006-04-10 a 2023-11-17 (dados observados) |
| ano | int | Ano | 1996 a 2023 |
| mes | int | Mês | 1 a 12 |
| trimestre | int | Trimestre | 1 a 4 |
| nome_mes | string | Nome do mês por extenso | Janeiro...Dezembro |
 
### gold.dim_desenvolvedora
 
Reúne os estúdios desenvolvedores distintos citados no catálogo — incluindo a própria Nintendo e
seus parceiros de longa data (Intelligent Systems, Retro Studios, entre outros). É a base da
Pergunta 5, sobre quais desenvolvedoras têm os jogos mais bem avaliados.
 
| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| desenvolvedora_id | int | Chave surrogate | — |
| nome_desenvolvedora | string | Nome do estúdio desenvolvedor | 207 valores distintos observados (ex.: Nintendo, Intelligent Systems, Retro Studios) |
 
### As tabelas-ponte: por que elas existem
 
Um jogo como *Super Smash Bros. Ultimate*, por exemplo, pode estar catalogado simultaneamente em
mais de um gênero (ação e luta) e ter mais de um estúdio envolvido no seu desenvolvimento. Se
essa informação fosse guardada só como texto dentro da própria `fato_jogos` (como uma lista, por
exemplo), não seria possível fazer um `JOIN` limpo com `dim_genero` ou `dim_desenvolvedora` sem
duplicar a linha do jogo na fato uma vez para cada combinação — o que inflaria a contagem de jogos
em qualquer agregação. A solução clássica de modelagem dimensional para esse tipo de relação
**N:N** é uma tabela-ponte: uma tabela simples, só com as duas chaves estrangeiras envolvidas, que
pode ser usada quando a análise precisa considerar *todos* os gêneros/desenvolvedoras de um jogo
(diferente da `fato_jogos`, que guarda só o gênero *primário* para consultas mais diretas).
 
### gold.ponte_jogo_genero
 
| Campo | Tipo | Descrição |
|---|---|---|
| jogo_id | bigint | FK para `fato_jogos` |
| genero_id | int | FK para `dim_genero` |
 
### gold.ponte_jogo_desenvolvedora
 
| Campo | Tipo | Descrição |
|---|---|---|
| jogo_id | bigint | FK para `fato_jogos` |
| desenvolvedora_id | int | FK para `dim_desenvolvedora` |
 
### Linhagem resumida (todas as tabelas)
Todas as tabelas Gold derivam de `silver.jogos_limpos`, que por sua vez deriva de
`bronze.jogos_raw` (cópia 1:1 do `NintendoGames.csv`, fonte: dataset
["Nintendo Games"](https://www.kaggle.com/datasets/joebeachcapital/nintendo-games) no Kaggle —
dados de metacritic.com, licença [Database Contents License (DbCL) v1.0](https://opendatacommons.org/licenses/dbcl/1-0/)).
Nenhuma tabela Gold recebe dados de fontes externas ao arquivo original.
 
---

## 4. Pipeline de Dados (Etapa 4.4)

### Como o pipeline foi organizado
 
O pipeline **não foi feito em um único notebook** — ele foi ramificado em **três notebooks
sequenciais**, um para cada camada da Arquitetura Medalhão (Bronze, Silver, Gold), mais dois
notebooks auxiliares (qualidade de dados e análise final). A decisão de ramificar, em vez de
concentrar tudo em um só notebook, foi tomada por três motivos práticos:
 
1. **Isolamento de responsabilidade.** Cada notebook tem um único propósito bem definido — o
   Bronze só ingere, o Silver só limpa e tipa, o Gold só modela — o que torna mais fácil entender
   o que cada etapa faz sem precisar ler o pipeline inteiro de uma vez.
2. **Reexecução independente.** Se eu precisar reprocessar só a modelagem Gold (por exemplo, para
   adicionar uma nova dimensão), não preciso rodar a ingestão e a limpeza de novo — basta reexecutar
   o notebook `03_gold_modelagem.py`, já que ele lê diretamente da tabela `silver.jogos_limpos`
   persistida.
3. **Rastreamento de erros.** Ao longo do desenvolvimento, essa separação foi o que permitiu
   isolar rapidamente em qual camada um problema estava ocorrendo (por exemplo, um erro de
   conversão de data que só acontecia na Silver, ou um erro de tipo que só aparecia no notebook de
   qualidade) — sem essa ramificação, depurar o pipeline inteiro de uma vez seria bem mais
   trabalhoso.
   
Cada notebook lê a tabela Delta persistida pelo notebook anterior (o Silver lê `bronze.jogos_raw`;
o Gold lê `silver.jogos_limpos`), formando uma cadeia onde a saída de uma camada é sempre a
entrada da próxima — e não uma sequência de células soltas dentro de um mesmo arquivo.

| Notebook | Camada | O que faz |
|---|---|---|
| [`01_bronze_ingestao.py`](notebooks/01_bronze_ingestao.py) | Bronze | Lê o CSV do Volume e persiste como Delta, sem transformar |
| [`02_silver_transformacao.py`](notebooks/02_silver_transformacao.py) | Silver | Remove duplicatas, corrige tipos, trata datas não-padrão, parseia arrays de gênero/desenvolvedora, padroniza ESRB |
| [`03_gold_modelagem.py`](notebooks/03_gold_modelagem.py) | Gold | Constrói o Esquema Estrela (fato + dimensões + pontes) |
| [`04_qualidade_dados.py`](notebooks/04_qualidade_dados.py) | — | Análise de qualidade sobre o dado bruto, documentando achados e tratamentos |
| [`05_analise.py`](notebooks/05_analise.py) | — | Consultas SQL sobre a Gold respondendo cada pergunta de negócio |

Os cinco notebooks estão disponíveis na íntegra no repositório GitHub, na pasta
[`notebooks/`](notebooks/), em formato de código-fonte do Databricks (podem ser reimportados
diretamente na plataforma).

### Transformações da camada Silver (`02_silver_transformacao.py`)
 
| O que foi feito | Por que foi feito | Impacto nos dados |
|---|---|---|
| Removi duplicatas usando `dropDuplicates(["title","platform"])`, pois a chave natural de um lançamento é o par jogo+plataforma | Garantir que cada linha represente um lançamento único, evitando contar o mesmo jogo mais de uma vez em qualquer análise | 2 registros duplicados eliminados, sem perda de informação real |
| Descartei o registro com `platform = "TG16)"` | Esse valor não corresponde a nenhuma plataforma Nintendo válida — é um resíduo de parsing da fonte original, e não é seguro inferir a plataforma correta a partir de 1 único registro | 1 registro removido; a coluna `platform` passa a conter apenas valores consistentes, permitindo agrupamentos confiáveis por plataforma |
| Criei a coluna `release_status` (Lancado / A anunciar / Cancelado) a partir do texto em `date`, testando se o valor tem formato de data válido | A coluna original mistura datas reais com textos como `"TBA"`, `"Canceled"` e `"Q4 2015"` — sem separar isso, qualquer tentativa de converter `date` para tipo Data quebraria o pipeline ou descartaria jogos que ainda não foram lançados | Nenhum jogo é perdido: os 30 registros sem data completa continuam no catálogo, agora com status explícito, em vez de serem silenciosamente descartados |
| Converti `date` para `release_date` (tipo Data), usando `try_to_date` apenas quando `release_status = "Lancado"` | Permitir cálculos temporais (ano, ordenação cronológica, filtros por período) que só fazem sentido sobre um tipo Data, não sobre texto livre | `release_date` fica `NULL` para jogos sem data confirmada, em vez de gerar erro de conversão ou um valor inventado |
| Converti `meta_score` e `user_score` de texto para número (`DoubleType`) | Essas colunas vêm como texto na Bronze por princípio (camada Bronze não tipa dados); sem essa conversão não é possível calcular médias, comparações ou correlações | Nulos continuam como `NULL` (ausência real da nota), e os valores numéricos passam a poder ser usados em agregações estatísticas |
| Transformei as colunas `genres` e `developers` (strings no formato `"['Action','Platformer']"`) em arrays reais do Spark, e extraí o primeiro item de `genres` como `primary_genre` | Um jogo pode ter mais de um gênero e mais de uma desenvolvedora; manter isso como texto impediria explodir essas listas em relações N:N na modelagem Gold | Cada jogo passa a ter um gênero primário para análises simples, e a lista completa de gêneros/desenvolvedoras fica disponível para as tabelas-ponte da camada Gold |
| Padronizei valores nulos/vazios de `esrb_rating` para o rótulo `"Nao informado"` | Deixar explícito, em qualquer agrupamento ou relatório, que a ausência da classificação é um dado da fonte original, e não um erro do pipeline | Nenhuma linha some de agregações por `esrb_rating`; os 122 jogos sem classificação continuam visíveis nas análises |  


<p align="center">
<img width="613" height="209" alt="image" src="https://github.com/user-attachments/assets/a387d46d-f794-4654-8fa4-d3622ee99583" />
 <br>
  <sub><i>Código e resultado da remoção de duplicatas</i></sub>
</p>

<p align="center">
<img width="643" height="210" alt="image" src="https://github.com/user-attachments/assets/003d3fa5-905d-473b-af29-1d853c543072" />
 <br>
  <sub><i>Código e resultado da padronização da coluna platform</i></sub>
</p>

<p align="center">
<img width="658" height="683" alt="image" src="https://github.com/user-attachments/assets/d56a6c73-e62b-49dc-a15e-b2c5e3c893e3" />
 <br>
  <sub><i>Código e resultado do tratamento da coluna date</i></sub>
</p>

<p align="center">
<img width="553" height="126" alt="image" src="https://github.com/user-attachments/assets/168fa876-f9b7-4b17-93c9-754cfdae44a8" />
 <br>
  <sub><i>Código da tipagem correta de meta_score e user_score</i></sub>
</p>

<p align="center">
<img width="746" height="382" alt="image" src="https://github.com/user-attachments/assets/f4378388-0433-4aa5-8d2a-1acd075b26ab" />
 <br>
  <sub><i>Código do parsing de genres e developers</i></sub>
</p>

<p align="center">
<img width="729" height="141" alt="image" src="https://github.com/user-attachments/assets/8ad8c4f5-9d43-49d4-9f3d-672f72acf603" />
 <br>
  <sub><i>Código de padronização de esrb_rating</i></sub>
</p>

<p align="center">
<img width="633" height="473" alt="image" src="https://github.com/user-attachments/assets/15d957a0-a820-4e22-97e8-0e44dfb45ba2" />
 <br>
  <sub><i>Código da seleção final e persistência Silver</i></sub>
</p>

<p align="center">
<img width="1531" height="283" alt="image" src="https://github.com/user-attachments/assets/adf46361-e915-43cd-9fc2-c1267f3fab5c" />
 <br>
  <sub><i>Tabela de demonstração da tabela de persistência Silver</i></sub>
</p>

### Transformações da camada Gold (`03_gold_modelagem.py`)
 
| O que foi feito | Por que foi feito | Impacto nos dados |
|---|---|---|
| Criei as dimensões `dim_plataforma`, `dim_genero`, `dim_classificacao_etaria` e `dim_data`, cada uma com uma chave surrogate própria | Um Esquema Estrela exige que atributos descritivos fiquem isolados em dimensões, em vez de repetidos em toda linha da fato — isso reduz redundância e centraliza a descrição de cada atributo (ex.: descrição por extenso de cada sigla ESRB) | As consultas de análise (notebook 05) passam a fazer `JOIN` simples entre fato e dimensão em vez de repetir texto bruto em cada linha |
| Fiz o `JOIN` entre `silver.jogos_limpos` e as quatro dimensões acima pelos respectivos nomes/valores, para montar a `fato_jogos` com as chaves substituindo os textos originais | Enriquecer cada linha de fato com as chaves corretas de cada dimensão, seguindo o padrão de Esquema Estrela — o mesmo princípio do exemplo do enunciado (JOIN de vendas com produtos por `product_id`), aqui aplicado a jogo × plataforma/gênero/classificação/data | A tabela fato fica compacta (só chaves + métricas), enquanto o significado de cada atributo mora nas dimensões, facilitando manutenção e consultas |
| Explodi (`explode`) os arrays `genres_array` e `developers_array` para criar as tabelas-ponte `ponte_jogo_genero` e `ponte_jogo_desenvolvedora`, junto com a nova dimensão `dim_desenvolvedora` | Um jogo pode ter vários gêneros e vários estúdios envolvidos — uma relação N:N não cabe como coluna única na tabela fato sem duplicar linhas | Passa a ser possível responder perguntas que consideram todos os gêneros/desenvolvedoras de um jogo (não só o primário), sem distorcer a contagem de jogos na fato |

---

## 5. Qualidade de Dados (Etapa 4.5)

A etapa de qualidade de dados existe porque nenhuma decisão de negócio pode ser mais confiável do
que os dados que a sustentam: se uma média de nota estiver distorcida por um valor inconsistente,
ou se uma data mal formatada quebrar uma agregação por ano, toda conclusão tirada a partir dali
carrega esse erro adiante — inclusive nas seis perguntas de negócio que este trabalho se propõe a
responder (Seção 1). É por isso que a qualidade de dados não fica isolada como uma checagem
pontual, e sim serve de ponte entre a Bronze (dado bruto, cru) e a Silver (dado confiável): é aqui
que decisões de tratamento são tomadas de forma consciente e documentada, em vez de deixadas para
serem descobertas (ou pior, não descobertas) durante a análise final.
 
Esse cuidado se mostrou especialmente relevante neste MVP porque o dataset cobre **27 anos de
lançamentos** (1996-2023), coletados de forma heterogênea ao longo do tempo — o que naturalmente
gera inconsistências: valores de data em formatos diferentes para jogos "a anunciar" ou
cancelados, uma plataforma registrada de forma inválida, notas ausentes para títulos mais antigos
ou de nicho. Sem uma etapa dedicada a identificar e tratar esses problemas antes da modelagem
Gold, análises como "evolução da nota média por ano" (Pergunta 4) ou "nota média por plataforma"
(Pergunta 1) correriam o risco de estar erradas sem que isso fosse perceptível à primeira vista —
o tipo de erro silencioso mais perigoso em um pipeline de dados, porque não gera uma falha visível,
apenas uma conclusão de negócio equivocada.

Análise completa no notebook [`notebooks/04_qualidade_dados.py`](notebooks/04_qualidade_dados.py).

| Dimensão | Problema encontrado | Tratamento aplicado |
|---|---|---|
| Completude | 35,2% de nulos em `meta_score`, 21,7% em `user_score`, 11,2% em `esrb_rating` | Mantidos como `NULL` (ausência real da nota) / rotulados `"Nao informado"`; nunca imputados com valor artificial |
| Unicidade | 2 duplicatas de `title`+`platform` | Removidas com `dropDuplicates` |
| Consistência | 1 valor de plataforma inválido (`TG16)`); 30 datas fora do padrão (`TBA`, `Canceled`, etc.) | Registro de plataforma inválida descartado; coluna `release_status` criada para separar situação de lançamento da data em si |
| Acurácia | Nenhum valor fora do domínio esperado (`meta_score` 37-99, `user_score` dentro de 0-10) | Não foi necessário tratamento |
| Outliers | 9 outliers identificados em `meta_score` pela regra do IQR (limite inferior 48 pontos) | Mantidos sem alteração: são notas baixas genuínas, dentro da escala válida (0-100), não erros de coleta |  

### Completude — valores nulos/vazios por coluna

<p align="center">
<img width="979" height="314" alt="image" src="https://github.com/user-attachments/assets/426bdf5d-bc97-4636-b56f-d4bbc1ab606a" />
 <br>
  <sub><i>Código e tabela com resultado da avaliação de Completude</i></sub>
</p>

**Achados (calculados sobre os 1094 registros):**  
meta_score: 385 nulos (35,2%) — muitos jogos, sobretudo mais antigos ou de nicho, nunca receberam nota consolidada da crítica no Metacritic.  
user_score: 238 nulos (21,7%) — jogos sem volume suficiente de avaliações de usuários.  
esrb_rating: 122 nulos (11,2%) — jogos sem classificação etária cadastrada na fonte.  
developers: 3 nulos. title, platform, date, genres: sem nulos.  
**Tratamento:** nulos em meta_score/user_score foram mantidos como NULL (ausência real da nota, não um erro — forçar um valor como 0 distorceria qualquer média). Nulos em esrb_rating foram padronizados para o rótulo "Nao informado" na Silver, para ficarem explícitos nas análises em vez de somem como NULL silencioso.<br><br>

### Unicidade — duplicatas

<p align="center">
<img width="624" height="327" alt="image" src="https://github.com/user-attachments/assets/cfa177bc-1275-4a16-ba69-c7c6353f63e1" />
 <br>
  <sub><i>Código e tabela com resultado da avaliação Unicidade</i></sub>
</p>

**Achado:** 2 pares duplicados de title+platform.  
**Tratamento:** removidos via dropDuplicates(["title","platform"]) na Silver, mantendo a primeira ocorrência.<br><br>

### Consistência — formato de platform e date

<p align="center">
<img width="613" height="410" alt="image" src="https://github.com/user-attachments/assets/e01cc862-04b2-4b3f-99e8-01d33c57e173" />
 <br>
  <sub><i>Código e tabela com resultado da avaliação Consistência</i></sub>
</p>

**Achado:** o valor TG16) aparece 1 vez e não corresponde a nenhuma plataforma Nintendo válida (resíduo de parsing da fonte original).  
**Tratamento:** registro descartado na Silver, com a decisão documentada (não é seguro inferir a plataforma correta a partir de 1 registro).<br><br>

<p align="center">
<img width="658" height="379" alt="image" src="https://github.com/user-attachments/assets/cc3b693b-7802-4931-8896-59cf315988ec" />
 <br>
  <sub><i>Código e tabela com resultado da avaliação Consistência</i></sub>
</p>

**Achado:** 30 registros com date fora do padrão (TBA, Canceled, TBA 2024, TBA 2011, TBA 2010, Q4 2015) — representam jogos anunciados mas não lançados, ou cancelados.  
**Tratamento:** criada a coluna release_status (Lancado / A anunciar / Cancelado) na Silver; release_date fica NULL para os que não têm data real, preservando a informação em vez de descartar a linha inteira.<br><br>

### Acurácia — faixas de valores esperadas

<p align="center">
<img width="770" height="431" alt="image" src="https://github.com/user-attachments/assets/b21263f0-ac74-4930-9963-6239f83ec56e" />
 <br>
  <sub><i>Código e tabela com resultado da avaliação Acurácia</i></sub>
</p>

**Achado:** meta_score varia de 37 a 99 (dentro da escala válida 0-100) e user_score fica dentro de 0-10.  
**Tratamento:** nenhum valor fora do domínio esperado foi encontrado — não foi necessário tratamento de acurácia nessas colunas.<br><br>

### Outliers

<p align="center">
<img width="843" height="248" alt="image" src="https://github.com/user-attachments/assets/a3380a34-ced1-4f98-9f8a-e7bfdae2587e" />
 <br>
  <sub><i>Código e tabela com resultado da avaliação Outliers</i></sub>
</p>

**Achado:** aplicando a regra do IQR (1,5x) sobre meta_score, foram identificados 9 outliers — jogos cuja nota de crítica fica abaixo do limite inferior calculado (48 pontos). O limite superior (104) fica acima da própria escala máxima do meta_score (100), então nenhum outlier ocorre por nota alta demais, apenas por nota baixa.  
**Tratamento:** esses 9 registros não foram removidos nem alterados, pois representam avaliações genuinamente baixas — dentro da escala válida do Metacritic (0 a 100) — e não erros de digitação ou de coleta. Remover esses jogos distorceria a análise ao esconder títulos legitimamente mal avaliados pela crítica, em vez de refletir a realidade do catálogo.<br><br>

## 6. Análise de Dados (Etapa 4.5)

Consultas completas com aplicação de consultas SQL, scripts Python e/ou visualizações para análise técnica podem ser encontradas em [`notebooks/05_analise.py`](notebooks/05_analise.py). 

As seguir são apresentadas as respostas (validadas sobre o dataset completo) para as perguntas de negócios realizadas no início deste trabalho:

**1. Qual plataforma Nintendo possui, em média, os jogos mais bem avaliados pela crítica?**

O N64 tem a melhor nota média de crítica da história da Nintendo: 83,97 pontos, em um catálogo pequeno de 31 jogos. Logo atrás vêm Game Boy Advance (79,0), Switch (78,0) e GameCube (77,9). Já 3DS e Wii, os catálogos maiores (centenas de jogos cada), ficam com médias mais baixas, em torno de 73,5. O padrão se repete em outras perguntas: quanto maior e mais variado é o catálogo de uma plataforma, mais a média se aproxima de um valor intermediário. O iOS teve a pior média (67,4), mas com apenas 14 jogos — amostra pequena demais para uma conclusão definitiva.

<p align="center">
<img width="683" height="574" alt="image" src="https://github.com/user-attachments/assets/c3c8f155-bd10-42f0-b174-5458c45b0206" />
 <br>
  <sub><i>Código e tabela com resultado da análise para a pergunta 1</i></sub>
</p>

**2. Existe correlação entre a nota da crítica (`meta_score`) e a nota dos usuários (`user_score`)?**

Na maior parte das vezes, sim, mas não sempre. Entre os 690 jogos que têm as duas notas, o nível de concordância entre crítica e usuários é de 0,625 em uma escala de 0 a 1 (quanto mais perto de 1, mais as duas notas andam juntas) — uma concordância forte, mas longe de ser total. Ou seja, um jogo bem avaliado pela crítica tem boas chances de agradar também os jogadores, mas existe um número relevante de exceções nos dois sentidos. As duas notas se complementam; nenhuma substitui a outra.

<p align="center">
<img width="503" height="268" alt="image" src="https://github.com/user-attachments/assets/94ca3e5a-919f-42a6-b89d-07dcfb006b37" />
 <br>
  <sub><i>Código e tabela com resultado da análise para a pergunta 2</i></sub>
</p>

**3. Quais gêneros de jogos concentram as maiores notas médias de crítica?**

Entre os gêneros com pelo menos 15 jogos lançados (para não deixar um único sucesso distorcer a média), os melhor avaliados são Strategy (82,1 em 91 jogos) e Action Adventure (81,3 em 63 jogos). Os RPGs (Role-Playing) vêm na sequência, com 77,9 de média em 139 jogos — o maior catálogo entre os gêneros bem avaliados. No outro extremo, Adventure (68,9) tem a pior média entre os gêneros relevantes, e Miscellaneous (72,5) — a categoria mais volumosa do catálogo, com 234 jogos — também fica abaixo da média geral.

<p align="center">
<img width="515" height="595" alt="image" src="https://github.com/user-attachments/assets/b1b4228a-90c1-4c2e-8a47-0ae0c0790240" />
 <br>
  <sub><i>Código e tabela com resultado da análise para a pergunta 3</i></sub>
</p>

**4. Como evoluíram o número de lançamentos e a nota média de crítica ao longo dos anos?**

Nos primeiros anos do dataset (1996 a 2002), a Nintendo lançava poucos jogos por ano, mas praticamente todos com nota acima de 80 pontos. A partir de meados dos anos 2000, o volume de lançamentos cresceu de forma acentuada — com picos em 2007 (86 jogos) e 2009 (90 jogos) —, e a nota média de crítica caiu e se estabilizou entre 73 e 78 pontos, patamar que se mantém até hoje. O dado não indica queda de qualidade da Nintendo como empresa; indica que, ao produzir mais jogos para públicos mais variados, a média do catálogo naturalmente se aproxima do centro — o mesmo efeito observado na Pergunta 1.

<p align="center">
<img width="485" height="693" alt="image" src="https://github.com/user-attachments/assets/0c733229-6b34-4ab7-9680-01fbfde0774a" />
<img width="485" height="329" alt="image" src="https://github.com/user-attachments/assets/48ee513f-a443-48c9-9d05-d3ca6e0a7230" />
 <br>
  <sub><i>Código e tabela com resultado da análise para a pergunta 4</i></sub>
</p>

**5. Quais desenvolvedoras (fora a própria Nintendo) produzem os jogos mais bem avaliados?**

Excluindo a própria Nintendo, a Retro Studios lidera com média de 89,4 pontos (10 jogos, incluindo a série Metroid Prime). Em seguida, empatadas, Monolith Soft e Rare Ltd., ambas com 83,7. Destaque para a Intelligent Systems (Fire Emblem, Advance Wars): além de manter nota alta (80,8), é a desenvolvedora terceira com maior volume no catálogo — 99 jogos —, mostrando que consegue qualidade consistente mesmo em grande escala.

<p align="center">
<img width="541" height="776" alt="image" src="https://github.com/user-attachments/assets/0f152c9d-46c3-4a67-be7c-3bc524ea8a9f" />
 <br>
  <sub><i>Código e tabela com resultado da análise para a pergunta 5</i></sub>
</p>

**6. A classificação etária (ESRB) influencia a nota média dos jogos?**

Não. As notas médias por classificação etária ficam dentro de uma faixa estreita: jogos T (Adolescentes) têm a maior média (79,5), seguidos por M (Maduro, 78,6) — base pequena, só 15 jogos —, E10+ (76,3) e E (Livre para todos, 75,7). A categoria E, de longe a mais comum no catálogo (660 dos 1.094 jogos, ou 6 em cada 10 lançamentos), fica apenas cerca de 4 pontos abaixo da líder. Ou seja, a classificação etária não é um fator determinante de qualidade no catálogo Nintendo.

<p align="center">
<img width="818" height="493" alt="image" src="https://github.com/user-attachments/assets/fc30381d-8111-4269-b309-f3ad2c6e4b57" />
 <br>
  <sub><i>Código e tabela com resultado da análise para a pergunta 6</i></sub>
</p>

**Conclusão geral**

O padrão que se repete nas seis respostas é o mesmo: volume e nota média caminham em direções opostas. Recortes menores e mais concentrados (N64, Strategy, Retro Studios, os primeiros anos da empresa) sustentam médias mais altas; recortes grandes e diversos (3DS, Wii, Miscellaneous, os anos de pico de lançamentos) tendem a ficar mais perto da média geral do catálogo. A classificação etária é a exceção: não segue esse padrão e não se mostrou um fator relevante de qualidade.

---

## 7. Autoavaliação

Desenvolver este MVP foi uma experiência enriquecedora e, ao mesmo tempo, bastante desafiadora. Consegui atingir os objetivos que tracei no início do trabalho: construí o pipeline de ponta a ponta, passando pelas cinco etapas propostas (definição do objetivo, coleta, modelagem, carga/ETL e análise), e cheguei ao final com as seis perguntas de negócio respondidas com dados reais extraídos do próprio pipeline. Mais do que isso, senti que consegui aplicar na prática boa parte dos conceitos vistos em aula — Arquitetura Medalhão, Lakehouse, Esquema Estrela, ETL — que até então eram só teoria para mim.

A maior dificuldade não foi entender os conceitos, mas lidar com problemas reais de dados durante a implementação, que é exatamente o tipo de imprevisto que o enunciado avisa que vai acontecer no dia a dia de um Engenheiro de Dados. Tive três erros na camada Silver e na camada de Qualidade de Dados que me obrigaram a rever minhas transformações: um valor de data fora do padrão esperado ("Q4 2015") que quebrou a conversão de datas porque minha lógica inicial só previa os casos "TBA" e "Canceled"; uma célula de notebook mal separada, misturando código Python com texto explicativo, que gerava erro de sintaxe; e uma tentativa de calcular estatísticas (quartis, outliers) diretamente sobre uma coluna que a camada Bronze mantém propositalmente como texto, sem conversão numérica. Resolver cada um desses problemas me fez entender na prática por que a documentação e o tratamento de exceções são tão importantes num pipeline de dados — no fim das contas, quase nenhum dataset do mundo real vem "limpo" como a gente espera.

Fiquei bastante satisfeito com o resultado final, pois atingi os objetivos traçados no início de trabalho, conseguindo responder todas as seis perguntas de negócios, mas principalmente por ter trabalhado com um tema que gosto de verdade, jogos eletrônicos. Isso deixou o processo de formular as perguntas de negócio e interpretar os resultados muito mais natural, porque eu já tinha familiaridade com o contexto (plataformas, desenvolvedoras, gêneros) e conseguia perceber quando um resultado fazia sentido ou merecia um olhar mais atento.

Como trabalhos futuros, pretendo explorar duas extensões deste MVP: cruzar este dataset com dados de vendas por título (por exemplo, do VGChartz) para relacionar nota da crítica com desempenho comercial, e não apenas com a recepção qualitativa; e aproveitar as tabelas-ponte que já construí (`ponte_jogo_genero`) para fazer uma análise multi-rótulo considerando todos os gêneros de cada jogo, e não apenas o gênero primário usado nesta versão do trabalho. Também vejo espaço para automatizar a ingestão (hoje manual, via upload de CSV) caso o dataset volte a ser atualizado pela fonte original.

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


