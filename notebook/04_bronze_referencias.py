# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze - Tabelas de referência
# MAGIC
# MAGIC Quatro tabelas que dão **nome** ao que o VRA guarda como **código**:
# MAGIC
# MAGIC | tabela | de onde vem | resolve |
# MAGIC | :--- | :--- | :--- |
# MAGIC | `voebem.bronze.aerodromos` | `AerodromosPublicos.csv` | `SBGR` → Guarulhos / São Paulo / SP |
# MAGIC | `voebem.bronze.empresas_nacionais` | `pda_empresas_aereas_nacionais.csv` | `GLO` → GOL LINHAS AÉREAS S.A. |
# MAGIC | `voebem.bronze.empresas_estrangeiras` | `pda_empresas_aereas_estrangeiros.csv` | `TAP` → TAP TRANSPORTES |
# MAGIC | `voebem.bronze.codigos_operacao` | seed table curada | `N` → Doméstica Mista |
# MAGIC
# MAGIC **Um arquivo, uma tabela.** Empresas aéreas chegam em **DOIS** cadastros da ANAC, e no bronze elas continuam em duas tabelas — bronze preserva o dado como chegou. Unir os dois é decisão de modelagem, e decisão de modelagem é trabalho da silver (marco-05).
# MAGIC
# MAGIC Cada arquivo tem uma armadilha diferente. Mesmo órgão, mesmo portal.

# COMMAND ----------

# Configuração Inicial para Tabelas de Referência

from pyspark.sql import functions as F

REF = "/Volumes/voebem/bronze/arquivos/referencias"

# caractere que NAO existe no arquivo -> desliga o quoting do leitor de CSV
SEM_ASPAS = chr(0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Aeródromos — latin-1 e aspas que não são aspas
# MAGIC
# MAGIC Duas armadilhas neste arquivo:
# MAGIC
# MAGIC `encoding = "ISO-8859-1"`. Este CSV não é UTF-8. Lido como UTF-8, "Plácido de Castro" vira "Pl?cido de Castro" — e aí o nome do aeroporto chega quebrado no produto final, que é justamente o que o cliente vai ler.
# MAGIC
# MAGIC `quote = SEM_ASPAS` (o caractere NUL, `chr(0)`). O arquivo **não usa aspas** para delimitar campo — mas usa o caractere `"` como símbolo de *segundo* nas coordenadas: `09°52'06"S`. Com o `quote='"'` padrão, o Spark abre uma aspa ali e sai engolindo linhas até achar a próxima. Desligar o quoting (apontando para um caractere que não existe no arquivo) é o que mantém uma linha = um registro.

# COMMAND ----------

# Ingestão da Tabela de Referência: Aeródromos

# Leitura do arquivo CSV de aeródromos públicos aplicando as correções de encoding e delimitadores
aerodromos = (
    spark.read.format("csv")
    .option("sep", ";")
    .option("header", "true")
    .option("skipRows", 1)
    .option("encoding", "ISO-8859-1")       # Garante a leitura correta de caracteres acentuados (latin-1)
    .option("quote", SEM_ASPAS)             # Desliga o quoting para evitar que o símbolo de segundos (") corrompa as linhas
    .load(f"{REF}/AerodromosPublicos.csv")
)

# Seleção, renomeação e padronização dos nomes das colunas para o formato Delta
aerodromos = aerodromos.select(
    F.col("`Código OACI`").alias("icao"),
    F.col("CIAD").alias("ciad"),
    F.col("Nome").alias("nome"),
    F.col("`Município`").alias("municipio"),
    F.col("UF").alias("uf"),
    F.col("`Município Servido`").alias("municipio_servido"),
    F.col("`UF Servido`").alias("uf_servido"),
    F.col("Latitude").alias("latitude"),
    F.col("Longitude").alias("longitude"),
    F.col("Altitude").alias("altitude"),
    F.col("`Situação`").alias("situacao"),
).withColumn("_ingerido_em", F.current_timestamp())  # Adiciona a coluna de auditoria com o carimbo de tempo da ingestão

# Escrita dos dados em formato Delta de forma idempotente (overwrite) na tabela gerenciada
aerodromos.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("voebem.bronze.aerodromos")

# Exibe a contagem total de linhas gravadas na tabela
print(f"bronze.aerodromos: {spark.table('voebem.bronze.aerodromos').count():,} linhas")

# Consulta de validação para verificar alguns aeródromos específicos
display(spark.sql("SELECT icao, nome, municipio, uf FROM voebem.bronze.aerodromos WHERE icao IN ('SBRB','SBGR','SBSP','SBFZ')"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Empresas — DOIS cadastros, DUAS tabelas
# MAGIC
# MAGIC A ANAC publica empresas aéreas em dois arquivos distintos, em duas pastas distintas do portal: um cadastro de empresas **nacionais** e outro de **estrangeiras**. São dois processos administrativos diferentes, com portarias diferentes.
# MAGIC
# MAGIC Os dois arquivos têm **exatamente** o mesmo cabeçalho (`"ICAO";"Estrangeira";"Razao";"Servico";...`), então um `UNION` poderia ser a escolha mais 'fácil', mas não a correta.
# MAGIC
# MAGIC E é exatamente por isso que a gente não faz.** No bronze, uma tabela por arquivo de origem. se a ANAC republicar só o de estrangeiras amanhã, eu não conseguiria reprocessar só ele e comparar com a versão anterior. Um `UNION` no bronze apaga a fronteira entre as duas fontes e me obriga a reprocessar as duas juntas para sempre.
# MAGIC
# MAGIC A unificação **vai acontecer** — é justamente o exemplo clássico de "mesmo assunto, dois sistemas". Só que ela é decisão de modelagem, e o lugar dela é a silver.
# MAGIC
# MAGIC Estes dois são UTF-8 com BOM e **usam** aspas de verdade — ou seja, configuração oposta à do arquivo anterior, que veio do mesmo portal.
# MAGIC

# COMMAND ----------

# Função de Leitura e Ingestão de Empresas (Nacionais e Estrangeiras)

def ler_empresas(arquivo: str):
    """Le um cadastro de empresas. Sem uniao, sem enriquecimento: uma tabela por arquivo."""
    return (
        spark.read.format("csv")
        .option("sep", ";")
        .option("header", "true")
        .option("skipRows", 1)
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .load(f"{REF}/{arquivo}")
        .select(
            F.col("ICAO").alias("icao"),
            F.col("Estrangeira").alias("sigla_iata"),
            F.col("Razao").alias("razao_social"),
            F.col("Servico").alias("servico"),
            F.col("Cidade").alias("cidade"),
            F.col("UF").alias("uf"),
            F.col("Ativa").alias("situacao"),
        )
        .withColumn("_arquivo_origem", F.lit(arquivo))
        .withColumn("_ingerido_em", F.current_timestamp())
    )

# Loop para processar e salvar os cadastros de empresas de forma idempotente
for arquivo, tabela in [
    ("pda_empresas_aereas_nacionais.csv",    "voebem.bronze.empresas_nacionais"),
    ("pda_empresas_aereas_estrangeiros.csv", "voebem.bronze.empresas_estrangeiras"),
]:
    ler_empresas(arquivo).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(tabela)
    print(f"{tabela}: {spark.table(tabela).count():,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC Duas tabelas, e a diferença entre elas já conta uma história:

# COMMAND ----------

# Consultas de Validação para Empresas Nacionais e Estrangeiras

# Consulta 1: Validação de registros específicos na tabela de empresas nacionais
display(spark.sql("""
    SELECT icao, razao_social, servico, uf, situacao
    FROM voebem.bronze.empresas_nacionais
    WHERE icao IN ('GLO','TAM','AZU','PAM')
    ORDER BY icao
"""))

# Consulta 2: Validação de registros específicos na tabela de empresas estrangeiras
display(spark.sql("""
    SELECT icao, razao_social, servico, situacao
    FROM voebem.bronze.empresas_estrangeiras
    WHERE icao IN ('AAL','TAP','AVA','ARG')
    ORDER BY icao
"""))

# Consulta 3: Validação consolidada usando UNION ALL para conferir o total de linhas e preenchimento da coluna icao em ambas as tabelas
display(spark.sql("""
    SELECT 'empresas_nacionais' AS tabela, COUNT(*) AS linhas,
           COUNT(CASE WHEN icao IS NOT NULL AND icao <> '' THEN 1 END) AS com_icao
    FROM voebem.bronze.empresas_nacionais
    UNION ALL
    SELECT 'empresas_estrangeiras', COUNT(*),
           COUNT(CASE WHEN icao IS NOT NULL AND icao <> '' THEN 1 END)
    FROM voebem.bronze.empresas_estrangeiras
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Códigos de operação — seed table
# MAGIC
# MAGIC O VRA guarda `codigo_di = "0"` e `codigo_tipo_linha = "N"`. Sem tradução, isso não significa nada para um analista — e significa menos ainda para um LLM.
# MAGIC
# MAGIC A ANAC publica essas descrições numa **página HTML**, não num CSV. Então esta tabela é uma *seed table*: dado de referência pequeno, estável e curado à mão, versionado junto com o código. É uma categoria legítima de fonte — o erro seria deixar esse mapeamento espalhado em `CASE WHEN` dentro das queries.

# COMMAND ----------

# Ingestão da Seed Table: Códigos de Operação

# Lista contendo os mapeamentos de domínio, códigos e suas respectivas descrições curadas manualmente
CODIGOS = [
    ("codigo_di", "0", "Etapa Regular"),
    ("codigo_di", "2", "Etapa Extra"),
    ("codigo_di", "3", "Etapa de Retorno"),
    ("codigo_di", "4", "Inclusão de Etapa"),
    ("codigo_di", "6", "Etapa Não Remunerada Sem Transporte de Objetos"),
    ("codigo_di", "7", "Etapa de Voo de Fretamento"),
    ("codigo_di", "9", "Etapa de Voo Charter"),
    ("codigo_di", "D", "Etapa de Voo Duplicada"),
    ("codigo_di", "E", "Etapa Não Remunerada Com Transporte de Objetos"),
    ("codigo_tipo_linha", "N", "Doméstica Mista"),
    ("codigo_tipo_linha", "C", "Doméstica Cargueira"),
    ("codigo_tipo_linha", "I", "Internacional Mista"),
    ("codigo_tipo_linha", "G", "Internacional Cargueira"),
]

# Cria um DataFrame do Spark a partir da lista de tuplas definindo explicitamente o esquema com três colunas string
codigos = spark.createDataFrame(CODIGOS, "dominio string, codigo string, descricao string")

# Salva o DataFrame como uma tabela Delta gerenciada no catálogo de forma idempotente (overwrite)
codigos.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("voebem.bronze.codigos_operacao")

# Imprime a contagem total de registros gravados na seed table
print(f"bronze.codigos_operacao: {spark.table('voebem.bronze.codigos_operacao').count()} linhas")

# Exibe o conteúdo completo da tabela de códigos gravada
display(spark.table("voebem.bronze.codigos_operacao"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. O bronze fechado
# MAGIC 4 arquivos de referência, 4 tabelas, nenhuma união

# COMMAND ----------

# Exibe todas as tabelas criadas dentro do esquema/database bronze para validação final da camada
display(spark.sql("SHOW TABLES IN voebem.bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Informações guardadas - Delta
# MAGIC
# MAGIC A gente nunca escreveu uma linha de código de versionamento. Mesmo assim:

# COMMAND ----------

display(spark.sql("""
    SELECT version, timestamp, operation,
           operationMetrics.numOutputRows AS linhas_escritas
    FROM (DESCRIBE HISTORY voebem.bronze.vra)
    ORDER BY version
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Time travel
# MAGIC
# MAGIC A versão 0 é a primeira carga do marco-03; a carga seguinte é a segunda execução da ingestão (aquela que provou a idempotência). Dá para consultar as duas lado a lado.

# COMMAND ----------

display(spark.sql("""
    SELECT 'versao 0 (1a carga)'   AS versao,
           COUNT(*)                AS linhas,
           MIN(_ingerido_em)       AS ingerido_em
    FROM voebem.bronze.vra VERSION AS OF 0
    UNION ALL
    SELECT 'versao atual', COUNT(*), MIN(_ingerido_em)
    FROM voebem.bronze.vra
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Mesmo número de linhas, `_ingerido_em` diferente: a prova de idempotência do marco anterior, agora reconstruída **do histórico**, sem ter guardado nada.
# MAGIC
# MAGIC Isso é propriedade do formato de tabela aberto, não código nosso. Todo `write` no Delta grava um commit no log de transações; o dado antigo continua nos arquivos Parquet até um `VACUUM`. Auditoria e rollback saem de graça.
# MAGIC

# COMMAND ----------

for tabela, comentario in [
    ("voebem.bronze.aerodromos",
     "Bronze - cadastro de aerodromos publicos da ANAC, como chegou. Chave: codigo ICAO (OACI). "
     "Cobre apenas aerodromos brasileiros - aeroportos estrangeiros do VRA nao estao aqui."),
    ("voebem.bronze.empresas_nacionais",
     "Bronze - cadastro de empresas aereas NACIONAIS da ANAC, como chegou. Chave: codigo ICAO. "
     "Nao unir com empresas_estrangeiras nesta camada: a uniao e feita na silver."),
    ("voebem.bronze.empresas_estrangeiras",
     "Bronze - cadastro de empresas aereas ESTRANGEIRAS autorizadas a operar no Brasil, como chegou. "
     "Chave: codigo ICAO. Cadastro separado do nacional na origem, mantido separado no bronze."),
    ("voebem.bronze.codigos_operacao",
     "Bronze - seed table curada a partir da pagina de descricao de variaveis da ANAC. "
     "Traduz codigo_di e codigo_tipo_linha para descricao em portugues."),
]:
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{comentario}'")

print("comentarios aplicados")