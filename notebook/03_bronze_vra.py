# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — VRA (Voo Regular Ativo)
# MAGIC
# MAGIC Lê os 12 CSVs mensais do volume `voebem.bronze.arquivos/vra/` e materializa `voebem.bronze.vra`.
# MAGIC
# MAGIC Regras da camada Bronze:
# MAGIC
# MAGIC - **nada de tipagem** — tudo string, exatamente como veio do arquivo;
# MAGIC - **nada de filtro** — nenhuma linha é descartada;
# MAGIC - **colunas de auditoria** — de qual arquivo veio e quando foi ingerido;
# MAGIC - **idempotente** — rodar duas vezes não duplica.

# COMMAND ----------

from pyspark.sql import functions as F 

CAMINHO = "/Volumes/voebem/bronze/arquivos/vra/*.csv"
TABELA = "voebem.bronze.vra"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura
# MAGIC
# MAGIC Quatro opções carregam problemas do arquivo:
# MAGIC
# MAGIC | opção | resolve |
# MAGIC | :--- | :--- |
# MAGIC | `sep=";"` | separador brasileiro, não vírgula |
# MAGIC | `skipRows=1` | a 1ª linha é `Atualizado em: <data>`, não o cabeçalho — e o BOM `EF BB BF` mora nela, some junto |
# MAGIC | `header=true` | a 2ª linha (a primeira que sobra) é o cabeçalho de verdade |
# MAGIC | `inferSchema` **desligado** (default) | bronze não tipa: tudo chega como `string` |

# COMMAND ----------

# Leitura do Dado Bruto (Camada Bronze)

bruto = (
    spark.read.format("csv")
    .option("sep", ";")                 # Define o separador de colunas como ponto e vírgula (padrão brasileiro)
    .option("header", "true")           # Considera a primeira linha restante como o cabeçalho das colunas
    .option("skipRows", 1)              # Descarta a 1ª linha (que contém "Atualizado em: ...") e remove o BOM junto
    .option("quote", "\"")              # Define o caractere de aspa dupla para tratar campos com textos delimitados
    .option("escape", "\"")             # Define o caractere de escape para lidar com aspas dentro dos campos
    .option("encoding", "UTF-8")        # Garante a codificação UTF-8 correta para acentuação e caracteres especiais
    .option("mode", "PERMISSIVE")       # Garante que nenhuma linha seja descartada/corrompida por erros de parsing (bronze não descarta linha)
    .load(CAMINHO)                      # Carrega os arquivos CSV a partir do caminho especificado na variável CAMINHO
)

print("Colunas lidas do arquivo")
for c in bruto.columns:
    print(f" - {c}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nomes de coluna: o Delta não aceita espaço
# MAGIC
# MAGIC `ICAO Empresa Aérea` é um nome de coluna válido em CSV e **inválido** em Delta, espaço está na lista de caracteres proibidos: 
# MAGIC ( `, ; { } ( ) \n \t = `).
# MAGIC
# MAGIC É preciso normalizar o **nome**. Repare que isso não fere a regra da bronze: o que a bronze preserva é o **valor** e a **granularidade**, não a grafia do cabeçalho. Nenhuma coluna é somada, removida, filtrada ou convertida.
# MAGIC
# MAGIC O mapa fica explícito no código. Nada de `regexp_replace` mágico, para que a correspondência com o arquivo original seja auditável.

# COMMAND ----------

# Renomear as colunas do DataFrame

RENOMEAR = {
    "ICAO Empresa Aérea": "icao_empresa",
    "Número Voo": "numero_voo",
    "Código Autorização (DI)": "codigo_di",
    "Código Tipo Linha": "codigo_tipo_linha",
    "ICAO Aeródromo Origem": "icao_origem",
    "ICAO Aeródromo Destino": "icao_destino",
    "Partida Prevista": "partida_prevista",
    "Partida Real": "partida_real",
    "Chegada Prevista": "chegada_prevista",
    "Chegada Real": "chegada_real",
    "Situação Voo": "situacao_voo",
    "Código Justificativa": "codigo_justificativa"
}

# Validação e Renomeação de Colunas na Camada Bronze
# Lista compreensiva para verificar se todas as colunas esperadas no dicionário RENOMEAR existem no DataFrame bruto
faltando = [c for c in RENOMEAR if c not in bruto.columns]

# Garante que a execução pare (lançando um erro) caso alguma coluna esperada não esteja presente no CSV
assert not faltando, f"Coluna esperada nao encontrada no CSV: {faltando}"

# Cria um novo DataFrame renomeando as colunas e garantindo que todas permaneçam como string (mantendo a regra da camada bronze)
renomeado = bruto.select(
    *[F.col(f"`{origem}`").cast("string").alias(novo) for origem, novo in RENOMEAR.items()]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auditoria
# MAGIC
# MAGIC Duas colunas que o arquivo não tem e a tabela precisa ter: `_arquivo_origem` (de qual CSV a linha veio — `_metadata` é uma coluna oculta que o Spark expõe em qualquer leitura de arquivo) e `_ingerido_em`.

# COMMAND ----------

bronze = renomeado.withColumn(
    "_arquivo_origem", F.col("_metadata.file_name")  # Extrai o nome do arquivo de origem usando a coluna oculta de metadados do Spark
).withColumn(
    "_ingerido_em", F.current_timestamp()          # Adiciona o carimbo de data e hora atual correspondente ao momento da ingestão
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escrita idempotente
# MAGIC
# MAGIC Estratégia: **full refresh determinístico** — `mode("overwrite")` sobre o conjunto inteiro de arquivos.
# MAGIC
# MAGIC Por que essa e não um `append` com deduplicação:
# MAGIC
# MAGIC 1. A fonte é **imutável e completa**: o volume tem os 12 arquivos do mês fechado, e a ANAC republica o mês inteiro quando corrige algo. A entrada define o estado final — logo o destino pode ser derivado inteiro dela.
# MAGIC 2. `append` exigiria uma chave de negócio para deduplicar. O VRA **não tem chave natural única** (o mesmo voo pode repetir legitimamente na mesma data — veja o código DI "Etapa de Voo Duplicada"). Deduplicar no bronze seria decidir regra de negócio na camada errada.
# MAGIC 3. `overwrite` no Delta é **atômico**: ou a versão nova aparece inteira, ou a antiga continua valendo. Ninguém lê tabela pela metade.
# MAGIC 4. O histórico não se perde: cada `overwrite` gera uma versão nova no log do Delta, e a anterior continua acessível por time travel (marco-04).
# MAGIC
# MAGIC O que muda entre duas execuções: só `_ingerido_em`. O **conjunto de linhas é idêntico** — é isso que a validação prova.

# COMMAND ----------

# Salvando a Tabela Delta e Verificando a Contagem

(
    bronze.write.format("delta")
    .mode("overwrite")                 # Sobrescreve os dados existentes de forma atômica (full refresh determinístico)
    .option("overwriteSchema", "true") # Permite atualizar o esquema da tabela caso tenha havido alguma mudança estrutural permitida
    .saveAsTable(TABELA)               # Salva o DataFrame como uma tabela Delta gerenciada no catálogo com o nome especificado em TABELA
)

# Imprime o nome da tabela formatado e a contagem total de linhas gravadas, separadas por vírgula para melhor leitura
print(f"{TABELA}: {spark.table(TABELA).count():,} linhas")

# COMMAND ----------

# Adicionando Documentação/Comentário à Tabela Delta

# Executa um comando SQL via PySpark para adicionar uma descrição/documentação formal à tabela Delta
spark.sql(f"""
    COMMENT ON TABLE {TABELA} IS
    'Bronze - VRA (Voo Regular Ativo) da ANAC, 12 meses (ago/2025 a jul/2026).
    Dado bruto: todas as colunas string, nenhuma linha descartada.
    Carga full refresh idempotente a partir de /Volumes/voebem/bronze/arquivos/vra/.'
""")

# COMMAND ----------

# Validação por Arquivo de Origem

# Exibe o resultado de uma consulta SQL para auditar a quantidade de linhas e o momento da ingestão por arquivo de origem
display(
    spark.sql(f"""
        SELECT _arquivo_origem, COUNT(*) AS linhas, MAX(_ingerido_em) AS ingerido_em
        FROM {TABELA}
        GROUP BY _arquivo_origem
        ORDER BY _arquivo_origem
    """)
)