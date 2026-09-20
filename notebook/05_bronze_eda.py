# Databricks notebook source
# DBTITLE 1,EDA - Qualidade de Dados Bronze/Silver
# MAGIC %md
# MAGIC # EDA — Análise de Qualidade de Dados (Bronze VRA + Silver Dimensionais)
# MAGIC
# MAGIC Análise exploratória para identificar problemas de qualidade que precisam ser tratados na camada silver. Avalia `voebem.bronze.vra`, `voebem.silver.empresas`, `voebem.silver.aerodromos` e `voebem.silver.codigos_operacao`. Não realiza transformações nem grava novas tabelas — apenas diagnostica e conclui.

# COMMAND ----------

# DBTITLE 1,Visão geral - contagem de linhas
# -*- coding: utf-8 -*-
"""Visão geral: volume de dados em cada tabela."""

tables = {
    "voebem.bronze.vra": "VRA (fato)",
    "voebem.silver.empresas": "Empresas (dimensão)",
    "voebem.silver.aerodromos": "Aerodromos (dimensão)",
    "voebem.silver.codigos_operacao": "Codigos de operacao (dimensão)",
}

rows = []
for tbl, label in tables.items():
    cnt = spark.sql(f"SELECT COUNT(*) AS n FROM {tbl}").collect()[0]["n"]
    rows.append((tbl, label, cnt))

display(spark.createDataFrame(rows, ["tabela", "descricao", "total_linhas"]))

# COMMAND ----------

# DBTITLE 1,VRA Bronze - Nulos por coluna
# -*- coding: utf-8 -*-
"""VRA Bronze: análise de nulos e valores vazios por coluna."""

vra_cols = [
    "icao_empresa", "numero_voo", "codigo_di", "codigo_tipo_linha",
    "icao_origem", "icao_destino", "partida_prevista", "partida_real",
    "chegada_prevista", "chegada_real", "situacao_voo", "codigo_justificativa",
]

exprs = []
for c in vra_cols:
    exprs.append(f"SUM(CASE WHEN {c} IS NULL OR TRIM({c}) = '' OR LOWER({c}) = 'null' THEN 1 ELSE 0 END) AS {c}_nulos")
    exprs.append(f"COUNT(*) AS {c}_total")

sql = f"""
SELECT {', '.join(exprs)}
FROM voebem.bronze.vra
"""

result = spark.sql(sql)
row = result.collect()[0]

data = []
for c in vra_cols:
    nulos = row[f"{c}_nulos"]
    total = row[f"{c}_total"]
    pct = round(nulos / total * 100, 2) if total else 0
    data.append((c, nulos, pct))

display(spark.createDataFrame(data, ["coluna", "registros_nulos_ou_vazios", "percentual_nulo_%"]))

# COMMAND ----------

# DBTITLE 1,VRA Bronze - Formato de datas
# -*- coding: utf-8 -*-
"""VRA Bronze: análise de formato das datas (todas as colunas são STRING no bronze)."""

# Verificar se as datas têm formato consistente e se 'partida_prevista' está sempre nula
spark.sql("""
SELECT
  SUM(CASE WHEN partida_prevista IS NULL OR TRIM(partida_prevista) = '' OR LOWER(partida_prevista) = 'null' THEN 1 ELSE 0 END) AS partida_prevista_nula,
  SUM(CASE WHEN partida_real IS NULL OR TRIM(partida_real) = '' OR LOWER(partida_real) = 'null' THEN 1 ELSE 0 END) AS partida_real_nula,
  SUM(CASE WHEN chegada_prevista IS NULL OR TRIM(chegada_prevista) = '' OR LOWER(chegada_prevista) = 'null' THEN 1 ELSE 0 END) AS chegada_prevista_nula,
  SUM(CASE WHEN chegada_real IS NULL OR TRIM(chegada_real) = '' OR LOWER(chegada_real) = 'null' THEN 1 ELSE 0 END) AS chegada_real_nula,
  COUNT(*) AS total
FROM voebem.bronze.vra
""").show()

# Amostra de formatos de data
spark.sql("""
SELECT DISTINCT
  CASE
    WHEN partida_real IS NULL OR LOWER(partida_real) = 'null' THEN 'NULL'
    ELSE regexp_extract(partida_real, r'^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$', 0)
  END AS formato_partida_real,
  CASE
    WHEN chegada_real IS NULL OR LOWER(chegada_real) = 'null' THEN 'NULL'
    ELSE regexp_extract(chegada_real, r'^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$', 0)
  END AS formato_chegada_real
FROM voebem.bronze.vra
LIMIT 20
""").show(truncate=False)

# Verificar se existe partida_prevista com valor válido em algum período
spark.sql("""
SELECT
  SUM(CASE WHEN partida_prevista IS NOT NULL AND TRIM(partida_prevista) != '' AND LOWER(partida_prevista) != 'null' THEN 1 ELSE 0 END) AS partida_prevista_valida,
  SUM(CASE WHEN chegada_prevista IS NOT NULL AND TRIM(chegada_prevista) != '' AND LOWER(chegada_prevista) != 'null' THEN 1 ELSE 0 END) AS chegada_prevista_valida,
  COUNT(*) AS total
FROM voebem.bronze.vra
""").show()

# COMMAND ----------

# DBTITLE 1,VRA Bronze - Distribuição categórica
# -*- coding: utf-8 -*-
"""VRA Bronze: distribuição de valores categóricos."""

# situacao_voo
spark.sql("""
SELECT situacao_voo, COUNT(*) AS n, ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM voebem.bronze.vra), 2) AS pct
FROM voebem.bronze.vra
GROUP BY situacao_voo
ORDER BY n DESC
""").show(truncate=False)

# codigo_di
spark.sql("""
SELECT codigo_di, COUNT(*) AS n
FROM voebem.bronze.vra
GROUP BY codigo_di
ORDER BY n DESC
""").show(truncate=False)

# codigo_tipo_linha
spark.sql("""
SELECT codigo_tipo_linha, COUNT(*) AS n
FROM voebem.bronze.vra
GROUP BY codigo_tipo_linha
ORDER BY n DESC
""").show(truncate=False)

# codigo_justificativa (top 20)
spark.sql("""
SELECT codigo_justificativa, COUNT(*) AS n
FROM voebem.bronze.vra
GROUP BY codigo_justificativa
ORDER BY n DESC
LIMIT 20
""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,VRA Bronze - Duplicatas e cobertura temporal
# -*- coding: utf-8 -*-
"""VRA Bronze: verificar duplicatas e cobertura temporal."""

# Duplicatas exatas (todas as colunas)
spark.sql("""
SELECT COUNT(*) AS total_linhas,
       COUNT(DISTINCT icao_empresa, numero_voo, codigo_di, codigo_tipo_linha,
             icao_origem, icao_destino, partida_real, chegada_real, situacao_voo) AS linhas_distintas
FROM voebem.bronze.vra
""").show()

# Duplicatas por chave de negócio: icao_empresa + numero_voo + partida_real
spark.sql("""
WITH dups AS (
  SELECT icao_empresa, numero_voo, partida_real, COUNT(*) AS cnt
  FROM voebem.bronze.vra
  WHERE partida_real IS NOT NULL AND LOWER(partida_real) != 'null'
  GROUP BY icao_empresa, numero_voo, partida_real
  HAVING COUNT(*) > 1
)
SELECT COUNT(*) AS grupos_duplicados, SUM(cnt) AS total_linhas_em_duplicatas, MAX(cnt) AS max_repeticoes
FROM dups
""").show()

# Cobertura temporal via _arquivo_origem
spark.sql("""
SELECT _arquivo_origem, COUNT(*) AS n, 
       MIN(partida_real) AS min_partida, MAX(partida_real) AS max_partida
FROM voebem.bronze.vra
WHERE partida_real IS NOT NULL AND LOWER(partida_real) != 'null'
GROUP BY _arquivo_origem
ORDER BY _arquivo_origem
""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Silver Empresas - Análise de qualidade
# -*- coding: utf-8 -*-
"""Silver Empresas: análise de qualidade."""

# Nulos e vazios por coluna
emp_cols = ["icao", "sigla_iata", "razao_social", "servico", "cidade", "uf", "situacao", "origem_cadastro"]
exprs = [f"SUM(CASE WHEN {c} IS NULL OR TRIM({c}) = '' THEN 1 ELSE 0 END) AS {c}_nulos" for c in emp_cols]
row = spark.sql(f"SELECT {', '.join(exprs)}, COUNT(*) AS total FROM voebem.silver.empresas").collect()[0]
total = row["total"]

data = [(c, row[f"{c}_nulos"], round(row[f"{c}_nulos"] / total * 100, 2)) for c in emp_cols]
display(spark.createDataFrame(data, ["coluna", "nulos", "percentual_%"]))

# Duplicidade de ICAO (chave de negócio para join com VRA)
spark.sql("""
SELECT
  SUM(CASE WHEN icao IS NULL OR TRIM(icao) = '' THEN 1 ELSE 0 END) AS icao_nulos,
  COUNT(DISTINCT icao) AS icao_distintos,
  COUNT(*) AS total
FROM voebem.silver.empresas
""").show()

# Verificar duplicatas de ICAO não-nulo
spark.sql("""
SELECT icao, COUNT(*) AS cnt
FROM voebem.silver.empresas
WHERE icao IS NOT NULL AND TRIM(icao) != ''
GROUP BY icao
HAVING COUNT(*) > 1
ORDER BY cnt DESC
""").show(truncate=False)

# Distribuição de origem_cadastro
spark.sql("""
SELECT origem_cadastro, COUNT(*) AS n
FROM voebem.silver.empresas
GROUP BY origem_cadastro
ORDER BY n DESC
""").show()

# Distribuição de situacao
spark.sql("""
SELECT situacao, COUNT(*) AS n
FROM voebem.silver.empresas
GROUP BY situacao
ORDER BY n DESC
""").show(truncate=False)

# sigla_iata com valor '..'
spark.sql("""
SELECT
  SUM(CASE WHEN sigla_iata = '..' THEN 1 ELSE 0 END) AS sigla_iata_dotdot,
  SUM(CASE WHEN sigla_iata IS NULL THEN 1 ELSE 0 END) AS sigla_iata_null,
  SUM(CASE WHEN sigla_iata IS NOT NULL AND sigla_iata != '..' THEN 1 ELSE 0 END) AS sigla_iata_valida,
  COUNT(*) AS total
FROM voebem.silver.empresas
""").show()

# COMMAND ----------

# DBTITLE 1,Silver Aerodromos - Análise de qualidade
# -*- coding: utf-8 -*-
"""Silver Aerodromos: análise de qualidade."""

aer_cols = ["icao", "ciad", "nome", "municipio", "uf_nome", "municipio_servido", "uf_servido_nome", "latitude_dms", "longitude_dms", "altitude_m", "situacao"]
exprs = [f"SUM(CASE WHEN {c} IS NULL OR TRIM(CAST({c} AS STRING)) = '' THEN 1 ELSE 0 END) AS {c}_nulos" for c in aer_cols]
row = spark.sql(f"SELECT {', '.join(exprs)}, COUNT(*) AS total FROM voebem.silver.aerodromos").collect()[0]
total = row["total"]

data = [(c, row[f"{c}_nulos"], round(row[f"{c}_nulos"] / total * 100, 2)) for c in aer_cols]
display(spark.createDataFrame(data, ["coluna", "nulos", "percentual_%"]))

# Duplicidade de ICAO
spark.sql("""
SELECT icao, COUNT(*) AS cnt
FROM voebem.silver.aerodromos
WHERE icao IS NOT NULL AND TRIM(icao) != ''
GROUP BY icao
HAVING COUNT(*) > 1
ORDER BY cnt DESC
""").show(truncate=False)

# Distribuição de situacao
spark.sql("""
SELECT situacao, COUNT(*) AS n
FROM voebem.silver.aerodromos
GROUP BY situacao
ORDER BY n DESC
""").show(truncate=False)

# Verificar se UF está como nome por extenso (não sigla)
spark.sql("""
SELECT DISTINCT uf_nome
FROM voebem.silver.aerodromos
ORDER BY uf_nome
""").show(50, truncate=False)

# Verificar valores anômalos em altitude_m
spark.sql("""
SELECT MIN(altitude_m) AS min_alt, MAX(altitude_m) AS max_alt,
       SUM(CASE WHEN altitude_m IS NULL THEN 1 ELSE 0 END) AS nulos_alt,
       SUM(CASE WHEN altitude_m < 0 OR altitude_m > 2000 THEN 1 ELSE 0 END) AS outliers_alt
FROM voebem.silver.aerodromos
""").show()

# COMMAND ----------

# DBTITLE 1,Silver Codigos Operacao - Análise de qualidade
# -*- coding: utf-8 -*-
"""Silver Codigos Operacao: análise de qualidade."""

# Nulos
spark.sql("""
SELECT
  SUM(CASE WHEN dominio IS NULL OR TRIM(dominio) = '' THEN 1 ELSE 0 END) AS dominio_nulos,
  SUM(CASE WHEN codigo IS NULL OR TRIM(codigo) = '' THEN 1 ELSE 0 END) AS codigo_nulos,
  SUM(CASE WHEN descricao IS NULL OR TRIM(descricao) = '' THEN 1 ELSE 0 END) AS descricao_nulos,
  COUNT(*) AS total
FROM voebem.silver.codigos_operacao
""").show()

# Duplicatas (dominio + codigo)
spark.sql("""
SELECT dominio, codigo, COUNT(*) AS cnt
FROM voebem.silver.codigos_operacao
GROUP BY dominio, codigo
HAVING COUNT(*) > 1
ORDER BY cnt DESC
""").show(truncate=False)

# Listar todos os registros
spark.sql("""
SELECT dominio, codigo, descricao
FROM voebem.silver.codigos_operacao
ORDER BY dominio, codigo
""").show(50, truncate=False)

# Valores distintos de dominio
spark.sql("""
SELECT dominio, COUNT(*) AS n
FROM voebem.silver.codigos_operacao
GROUP BY dominio
ORDER BY n DESC
""").show()

# COMMAND ----------

# DBTITLE 1,Integridade referencial cruzada
# -*- coding: utf-8 -*-
"""Integridade referencial cruzada: VRA vs dimensões silver."""

# 1. VRA.icao_empresa -> Empresas.icao
spark.sql("""
WITH vra_empresas AS (
  SELECT DISTINCT icao_empresa FROM voebem.bronze.vra
  WHERE icao_empresa IS NOT NULL AND TRIM(icao_empresa) != ''
),
silver_empresas AS (
  SELECT DISTINCT icao FROM voebem.silver.empresas
  WHERE icao IS NOT NULL AND TRIM(icao) != ''
)
SELECT
  (SELECT COUNT(*) FROM vra_empresas) AS icaos_empresa_no_vra,
  (SELECT COUNT(*) FROM vra_empresas WHERE icao_empresa NOT IN (SELECT icao FROM silver_empresas)) AS sem_match_empresas,
  (SELECT COUNT(*) FROM vra_empresas WHERE icao_empresa IN (SELECT icao FROM silver_empresas)) AS com_match_empresas
""").show()

# ICAO de empresa no VRA sem match na silver
spark.sql("""
SELECT DISTINCT v.icao_empresa
FROM voebem.bronze.vra v
LEFT JOIN voebem.silver.empresas e ON v.icao_empresa = e.icao
WHERE v.icao_empresa IS NOT NULL AND TRIM(v.icao_empresa) != '' AND e.icao IS NULL
ORDER BY v.icao_empresa
""").show(truncate=False)

# 2. VRA.icao_origem + icao_destino -> Aerodromos.icao
spark.sql("""
WITH vra_aerodromos AS (
  SELECT DISTINCT icao FROM (
    SELECT icao_origem AS icao FROM voebem.bronze.vra WHERE icao_origem IS NOT NULL AND TRIM(icao_origem) != ''
    UNION
    SELECT icao_destino AS icao FROM voebem.bronze.vra WHERE icao_destino IS NOT NULL AND TRIM(icao_destino) != ''
  )
),
silver_aerodromos AS (
  SELECT DISTINCT icao FROM voebem.silver.aerodromos
  WHERE icao IS NOT NULL AND TRIM(icao) != ''
)
SELECT
  (SELECT COUNT(*) FROM vra_aerodromos) AS icaos_aerodromo_no_vra,
  (SELECT COUNT(*) FROM vra_aerodromos WHERE icao NOT IN (SELECT icao FROM silver_aerodromos)) AS sem_match_aerodromos,
  (SELECT COUNT(*) FROM vra_aerodromos WHERE icao IN (SELECT icao FROM silver_aerodromos)) AS com_match_aerodromos
""").show()

# ICAO de aerodromo no VRA sem match na silver (top 30)
spark.sql("""
SELECT DISTINCT icao, COUNT(*) AS vezes_no_vra
FROM (
  SELECT icao_origem AS icao FROM voebem.bronze.vra
  UNION ALL
  SELECT icao_destino AS icao FROM voebem.bronze.vra
) t
WHERE icao IS NOT NULL AND TRIM(icao) != ''
  AND icao NOT IN (SELECT icao FROM voebem.silver.aerodromos WHERE icao IS NOT NULL AND TRIM(icao) != '')
GROUP BY icao
ORDER BY vezes_no_vra DESC
LIMIT 30
""").show(truncate=False)

# 3. VRA.codigo_di -> Codigos_operacao (dominio = 'codigo_di')
spark.sql("""
WITH vra_di AS (
  SELECT DISTINCT codigo_di FROM voebem.bronze.vra WHERE codigo_di IS NOT NULL AND TRIM(codigo_di) != ''
),
silver_di AS (
  SELECT DISTINCT codigo FROM voebem.silver.codigos_operacao WHERE dominio = 'codigo_di'
)
SELECT
  (SELECT COUNT(*) FROM vra_di) AS codigos_di_no_vra,
  (SELECT COUNT(*) FROM vra_di WHERE codigo_di NOT IN (SELECT codigo FROM silver_di)) AS sem_match_di
""").show()

spark.sql("""
SELECT DISTINCT v.codigo_di
FROM voebem.bronze.vra v
LEFT JOIN voebem.silver.codigos_operacao c ON v.codigo_di = c.codigo AND c.dominio = 'codigo_di'
WHERE v.codigo_di IS NOT NULL AND TRIM(v.codigo_di) != '' AND c.codigo IS NULL
ORDER BY v.codigo_di
""").show(truncate=False)

# 4. VRA.codigo_tipo_linha -> Codigos_operacao (dominio = 'codigo_tipo_linha')
spark.sql("""
WITH vra_tl AS (
  SELECT DISTINCT codigo_tipo_linha FROM voebem.bronze.vra WHERE codigo_tipo_linha IS NOT NULL AND TRIM(codigo_tipo_linha) != ''
),
silver_tl AS (
  SELECT DISTINCT codigo FROM voebem.silver.codigos_operacao WHERE dominio = 'codigo_tipo_linha'
)
SELECT
  (SELECT COUNT(*) FROM vra_tl) AS codigos_tipo_linha_no_vra,
  (SELECT COUNT(*) FROM vra_tl WHERE codigo_tipo_linha NOT IN (SELECT codigo FROM silver_tl)) AS sem_match_tipo_linha
""").show()

spark.sql("""
SELECT DISTINCT v.codigo_tipo_linha
FROM voebem.bronze.vra v
LEFT JOIN voebem.silver.codigos_operacao c ON v.codigo_tipo_linha = c.codigo AND c.dominio = 'codigo_tipo_linha'
WHERE v.codigo_tipo_linha IS NOT NULL AND TRIM(v.codigo_tipo_linha) != '' AND c.codigo IS NULL
ORDER BY v.codigo_tipo_linha
""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Conclusões
# MAGIC %md
# MAGIC # Conclusões da Análise de Qualidade de Dados
# MAGIC
# MAGIC ## 1. VRA Bronze (`voebem.bronze.vra`) — 1.014.705 linhas
# MAGIC
# MAGIC ### 1.1 Tipagem: todas as colunas são STRING
# MAGIC Todas as colunas de negócio estão como STRING no bronze, incluindo as 4 colunas de data/hora (`partida_prevista`, `partida_real`, ` `chegada_prevista`, `chegada_real`). O formato é consistente (`yyyy-MM-dd HH:mm:ss`), mas precisam ser convertidas para **TIMESTAMP** na silver.
# MAGIC
# MAGIC ### 1.2 Nulos nas datas
# MAGIC | Coluna | Nulos | % | Observação |
# MAGIC |---|---|---|---|
# MAGIC | `partida_prevista` | 30.800 | 3,04% | Inclui 1.655 voos REALIZADO sem prevista |
# MAGIC | `partida_real` | 29.145 | 2,87% | Coincide exatamente com voos CANCELADO |
# MAGIC | `chegada_prevista` | 30.800 | 3,04% | Mesmo padrão da partida_prevista |
# MAGIC | `chegada_real` | 29.145 | 2,87% | Coincide com CANCELADO |
# MAGIC
# MAGIC **Tratamento na silver:** converter as 4 colunas para TIMESTAMP. Os 29.145 nulos em `partida_real`/`chegada_real` correspondem aos voos CANCELADO — são nulos legítimos, não defeito. Os 1.655 voos REALIZADO sem `partida_prevista`/`chegada_prevista` merecem atenção: pode ser lacuna na origem (ANAC não publicou a prevista).
# MAGIC
# MAGIC ### 1.3 Duplicatas: 24.059 linhas duplicadas (2,37%)
# MAGIC Foram encontradas **1.014.705 linhas** contra **990.646 distintas** (todas as colunas). A causa raiz é o **sobreposicão entre arquivos mensais**: cada CSV da ANAC cobre do final do mês anterior ao início do próximo, gerando duplicação nas bordas. Há também **118 grupos duplicados** pela chave de negócio (icao_empresa + numero_voo + partida_real), com máximo de 2 repetições.
# MAGIC
# MAGIC **Tratamento na silver:** desduplicar com `ROW_NUMBER() OVER (PARTITION BY todas_colunas ORDER BY _ingerido_em DESC)` mantendo a ingestão mais recente. Considerar também se a desduplicação por chave de negócio (empresa + voo + data) é necessária.
# MAGIC
# MAGIC ### 1.4 `codigo_justificativa` — sempre "N/A"
# MAGIC Todas as 1.014.705 linhas têm o valor literal `"N/A"`. Esta coluna **não aporta informação** e pode ser descartada na silver ou mantida apenas para fidelidade ao schema original.
# MAGIC
# MAGIC ### 1.5 Domínios categóricos — consistentes
# MAGIC * `situacao_voo`: apenas `REALIZADO` (97,13%) e `CANCELADO` (2,87%) — domínio fechado e limpo.
# MAGIC * `codigo_di`: 10 valores distintos (0, 1, 2, 3, 4, 6, 7, 9, D, E) — ver item 5.1 abaixo.
# MAGIC * `codigo_tipo_linha`: 4 valores (N, I, G, C) — todos com descrição na silver.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Silver Empresas (`voebem.silver.empresas`) — 878 linhas
# MAGIC
# MAGIC ### 2.1 `icao` — 80,87% nulos (710 de 878)
# MAGIC A maior parte dos operadores (aviação agrícola, táxi aéreo, aeroclube) não tem código ICAO. Apenas **168 registros** têm ICAO não-nulo, **sem duplicatas**. Isso é uma propriedade da fonte, não um defeito, mas **impacta a join com o VRA** (ver item 5.2).
# MAGIC
# MAGIC ### 2.2 `sigla_iata` — 63,67% com valor `".."`
# MAGIC O valor `".."` (dois pontos) é usado pela ANAC como placeholder para "sem código IATA". São **559 registros** com `".."`, **152 nulos** e apenas **167 válidos**. Na silver, recomenda-se **padronizar `".."` para NULL** para evitar ambiguidade em filtros.
# MAGIC
# MAGIC ### 2.3 Sem outros problemas
# MAGIC `razao_social`, `servico`, `cidade`, `uf`, `situacao`, `origem_cadastro` — **0 nulos**. `situacao` é sempre `ATIVA`. `origem_cadastro`: 729 nacional + 149 estrangeira.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. Silver Aerodromos (`voebem.silver.aerodromos`) — 496 linhas
# MAGIC
# MAGIC ### 3.1 Nulos em `municipio` e `uf_nome`
# MAGIC **5 registros (1,01%)** têm `municipio` e `uf_nome` nulos, e **2 registros (0,4%)** têm `municipio_servido` e `uf_servido_nome` nulos. Sugere-se investigar se são aerodromos sem localização cadastrada ou erro de preenchimento da ANAC.
# MAGIC
# MAGIC ### 3.2 `uf_nome` por extenso, não sigla
# MAGIC A coluna `uf_nome` traz o nome da UF por extenso (`São Paulo`, `Rio de Janeiro`, `Pará`). Há **1 registro com NULL** nesta coluna. Não há coluna com a sigla da UF — se necessária para joins ou filtros, será preciso derivar.
# MAGIC
# MAGIC ### 3.3 `altitude_m` — sem outliers
# MAGIC Valores entre 0,0 e 1.426,0 metros, sem nulos e sem outliers plausíveis.
# MAGIC
# MAGIC ### 3.4 Sem duplicatas de ICAO
# MAGIC Os 496 registros têm ICAO único — **sem duplicidade**.
# MAGIC
# MAGIC ### 3.5 `situacao`: 465 Cadastrado + 31 Interditado
# MAGIC 31 aeródromos interditados estão na tabela. Se a análise for apenas de aeródromos operacionais, esses podem precisar de filtro, mas mantê-los é correto pois podem ter voado no período.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. Silver Codigos Operacao (`voebem.silver.codigos_operacao`) — 13 linhas
# MAGIC
# MAGIC ### 4.1 Sem problemas de qualidade
# MAGIC 0 nulos, 0 duplicatas. A tabela cobre 9 códigos DI e 4 códigos de tipo de linha.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. Integridade Referencial Cruzada
# MAGIC
# MAGIC ### 5.1 VRA → Codigos Operacao
# MAGIC | Dimensão | Códigos no VRA | Sem match na silver |
# MAGIC |---|---|---|
# MAGIC | `codigo_di` | 10 | **1** (código `"1"`) |
# MAGIC | `codigo_tipo_linha` | 4 | 0 |
# MAGIC
# MAGIC O código `codigo_di = "1"` aparece em **6.338 voos** no VRA mas **não tem descrição** na tabela `codigos_operacao`. A tabela silver precisa deste código ou o VRA precisa ser tratado.
# MAGIC
# MAGIC ### 5.2 VRA.icao_empresa → Silver Empresas
# MAGIC De 118 ICAO de empresa no VRA, **7 não têm match** na silver: `1ED, AXY, EPT, GJW, MSI, TXG, VVT`. São 111 matches válidos. Essas 7 empresas podem estar cadastradas sem ICAO na silver (ou ser empresas estrangeiras ausentes do cadastro). Recomenda-se investigar e enriquecer a dimensão.
# MAGIC
# MAGIC ### 5.3 VRA.icao_origem/destino → Silver Aerodromos
# MAGIC De 396 ICAO de aeródromo no VRA, **234 não têm match** na silver. Os top 20 são todos **aeroportos estrangeiros**: SCEL (Santiago), SAEZ (Buenos Aires), KMIA (Miami), LFPG (Paris), KJFK (Nova York), etc. Isso é **esperado pela design** da tabela silver (apenas aeródromos brasileiros). Se a silver do VRA precisar enriquecer origem/destino, será necessário lidar com os estrangeiros separadamente (dimensão complementar ou `LEFT JOIN` com NULL aceitável).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Resumo — O que tratar na Silver do VRA
# MAGIC
# MAGIC | # | Problema | Severidade | Ação recomendada |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Datas como STRING | Alta | Converter `partida_prevista`, `partida_real`, `chegada_prevista`, `chegada_real` para TIMESTAMP |
# MAGIC | 2 | 24.059 linhas duplicadas | Alta | Desduplicar por todas as colunas, mantendo ingestão mais recente |
# MAGIC | 3 | `codigo_justificativa` sempre "N/A" | Baixa | Manter por fidelidade ou descartar (sem valor analítico) |
# MAGIC | 4 | `codigo_di = "1"` sem descrição | Média | Adicionar descrição à `codigos_operacao` ou tratar como "não catalogado" |
# MAGIC | 5 | 7 ICAO de empresa sem match | Média | Investigar e enriquecer dimensão de empresas |
# MAGIC | 6 | 234 ICAO de aeródromo estrangeiro sem match | Informacional | Por design; tratar com LEFT JOIN ou dimensão complementar |
# MAGIC | 7 | 1.655 voos REALIZADO sem data prevista | Baixa | Manter; lacuna na origem ANAC |
# MAGIC | 8 | `sigla_iata = ".."` em empresas | Baixa | Padronizar para NULL na silver |
# MAGIC | 9 | 5 aeródromos sem município/UF | Baixa | Investigar na fonte ANAC |