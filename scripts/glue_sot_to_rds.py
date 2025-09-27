# glue_sot_to_rds_fullload.py
# Envia TODAS as linhas do SOT (Parquet no S3) para o RDS (tabela pnad.sot)
# - Sem dropDuplicates (não perde linhas)
# - Confere contagens por ano/mes antes de gravar
# - Opção de overwrite (truncate) controlada por parâmetro
# - Parâmetros úteis para desempenho: BATCHSIZE e COALESCE

import sys
from pyspark.sql import SparkSession, functions as F, types as T

# ---------------------------
# Args helpers
# ---------------------------
def arg(flag, default=None):
    f = f"--{flag}"
    if f in sys.argv:
        i = sys.argv.index(f)
        return sys.argv[i+1] if i + 1 < len(sys.argv) else ""
    return default

SOT_PATH   = arg("SOT_PATH", "s3://pnad-sot/")              # prefixo raiz do SOT
JDBC_URL   = arg("JDBC_URL", "jdbc:postgresql://<endpoint>:5432/<db>")
DB_USER    = arg("DB_USER",  "postgres")
DB_PWD     = arg("DB_PWD",   "SUA_SENHA_AQUI")              # em produção: Secrets Manager
OVERWRITE  = (arg("OVERWRITE", "false") or "false").lower() == "true"
BATCHSIZE  = arg("BATCHSIZE", "20000")                      # ajuste se necessário
COALESCE_N = int(arg("COALESCE", "8"))                      # n de partições para JDBC

print("=== ARGS DEBUG ===")
print("SOT_PATH :", SOT_PATH)
print("JDBC_URL :", JDBC_URL)
print("DB_USER  :", DB_USER)
print("OVERWRITE:", OVERWRITE, "| BATCHSIZE:", BATCHSIZE, "| COALESCE:", COALESCE_N)
print("==================")

spark = (SparkSession.builder
         .appName("glue_sot_to_rds_fullload")
         .getOrCreate())

props = {"user": DB_USER, "password": DB_PWD, "driver": "org.postgresql.Driver"}

# ---------------------------
# Ler SOT (Parquet)
# ---------------------------
df = spark.read.parquet(SOT_PATH)

EXPECTED = [
    "situacao_domicilio","sexo","escolaridade","faixa_idade",
    "febre","tosse","dificuldade_respirar","perda_olfato_paladar","fadiga",
    "procurou_atendimento","atendimento_sus_hospital","atendimento_priv_hospital",
    "internacao","sedado_entubado",
    "auxilio_emergencial","seguro_desemprego","bolsa_familia",
    "ano","mes","uf_sigla"
]

missing = [c for c in EXPECTED if c not in df.columns]
if missing:
    raise Exception(f"[ERRO] Colunas faltando na SOT: {missing}")

df_out = (
    df.select(*EXPECTED)
      # tipagem final coerente com o RDS
      .withColumn("ano", F.col("ano").cast(T.IntegerType()))
      .withColumn("mes", F.col("mes").cast(T.IntegerType()))
      .withColumn("uf_sigla", F.col("uf_sigla").cast(T.StringType()))
      .withColumn("situacao_domicilio", F.col("situacao_domicilio").cast(T.StringType()))
      .withColumn("sexo", F.col("sexo").cast(T.StringType()))
      .withColumn("escolaridade", F.col("escolaridade").cast(T.StringType()))
      .withColumn("faixa_idade", F.col("faixa_idade").cast(T.StringType()))
      .withColumn("febre", F.col("febre").cast(T.StringType()))
      .withColumn("tosse", F.col("tosse").cast(T.StringType()))
      .withColumn("dificuldade_respirar", F.col("dificuldade_respirar").cast(T.StringType()))
      .withColumn("perda_olfato_paladar", F.col("perda_olfato_paladar").cast(T.StringType()))
      .withColumn("fadiga", F.col("fadiga").cast(T.StringType()))
      .withColumn("procurou_atendimento", F.col("procurou_atendimento").cast(T.StringType()))
      .withColumn("atendimento_sus_hospital", F.col("atendimento_sus_hospital").cast(T.StringType()))
      .withColumn("atendimento_priv_hospital", F.col("atendimento_priv_hospital").cast(T.StringType()))
      .withColumn("internacao", F.col("internacao").cast(T.StringType()))
      .withColumn("sedado_entubado", F.col("sedado_entubado").cast(T.StringType()))
      .withColumn("auxilio_emergencial", F.col("auxilio_emergencial").cast(T.StringType()))
      .withColumn("seguro_desemprego", F.col("seguro_desemprego").cast(T.StringType()))
      .withColumn("bolsa_familia", F.col("bolsa_familia").cast(T.StringType()))
)

# ---------------------------
# Conferência (deve bater com Athena)
# ---------------------------
print("Contagem por ano/mes (pré-write):")
df_out.groupBy("ano","mes").count().orderBy("ano","mes").show(50, False)
print("TOTAL (pré-write):", df_out.count())

# ---------------------------
# Write JDBC -> pnad.sot
# ---------------------------
target_table = "pnad.sot"

writer = (df_out
          .coalesce(COALESCE_N)  # reduz conexões/commits no JDBC
          .write
          .format("jdbc")
          .option("url", JDBC_URL)
          .option("dbtable", target_table)
          .options(**props)
          .option("batchsize", BATCHSIZE)
          .option("isolationLevel", "NONE")
          .option("ssl", "true")
          .option("sslmode", "require"))

if OVERWRITE:
    # usa overwrite com truncate=true para substituir o conteúdo da tabela
    (writer
     .option("truncate", "true")
     .mode("overwrite")
     .save())
    print("=== OVERWRITE OK ->", target_table, "===")
else:
    # append simples (não faz dedupe)
    writer.mode("append").save()
    print("=== APPEND OK ->", target_table, "===")
