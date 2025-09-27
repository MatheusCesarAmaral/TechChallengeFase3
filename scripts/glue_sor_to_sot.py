# Glue Job: SOR (CSV) -> SOT (Parquet) v2.5 (Glue 5.0)
# - Lê CSV da SOR e grava Parquet na SOT
# - Extrai ano/mes do caminho (fail-fast se não achar)
# - Particiona por ano/mes/uf_sigla
# - **Saída = 20 variáveis já renomeadas e COM RÓTULOS DE TEXTO**
# - Compatível com header/no-header (INDEX_MAP opcional)

import sys, json
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F, types as T
from itertools import chain

# ---------------- args ----------------
def get_arg(name, default=None):
    flag = f"--{name}"
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv): return sys.argv[i + 1]
        return ""
    return default

args = getResolvedOptions(sys.argv, ["SOR_PATH", "SOT_PATH"])

DELIM          = (get_arg("DELIMITER", ",") or ",").strip()
HAS_HEADER     = (get_arg("HAS_HEADER", "true") or "true").strip().lower() == "true"
FAIL_ON_MISSING= (get_arg("FAIL_ON_MISSING", "false") or "false").strip().lower() == "true"
INDEX_MAP_STR  = (get_arg("INDEX_MAP", "{}") or "{}").strip()

SOR_PATH = (args.get("SOR_PATH") or "").strip()
SOT_PATH = (args.get("SOT_PATH") or "").strip()

print("=== ARGS DEBUG ===")
print(f"SOR_PATH='{SOR_PATH}'")
print(f"SOT_PATH='{SOT_PATH}'")
print(f"DELIMITER='{DELIM}'  HAS_HEADER={HAS_HEADER}  FAIL_ON_MISSING={FAIL_ON_MISSING}")
print("==================")

if not SOR_PATH.startswith("s3://"): raise Exception("SOR_PATH inválido (use s3://...)")
if not SOT_PATH.startswith("s3://"): raise Exception("SOT_PATH inválido (use s3://...)")

try:
    INDEX_MAP = json.loads(INDEX_MAP_STR)
except Exception:
    INDEX_MAP = {}

# ---------------- spark ----------------
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ---------------- mapeamentos ----------------
UF_MAP = {
    "11":"RO","12":"AC","13":"AM","14":"RR","15":"PA","16":"AP","17":"TO",
    "21":"MA","22":"PI","23":"CE","24":"RN","25":"PB","26":"PE","27":"AL","28":"SE","29":"BA",
    "31":"MG","32":"ES","33":"RJ","35":"SP","41":"PR","42":"SC","43":"RS",
    "50":"MS","51":"MT","52":"GO","53":"DF"
}
uf_pairs  = list(chain.from_iterable([[F.lit(k), F.lit(v)] for k,v in UF_MAP.items()]))
map_uf    = F.create_map(uf_pairs)

map_v1022 = F.create_map(F.lit("1"), F.lit("Urbano"), F.lit("2"), F.lit("Rural"))
map_sexo  = F.create_map(F.lit("1"), F.lit("Masculino"), F.lit("2"), F.lit("Feminino"))

ESC_MAP = {
    "1":"Sem instrução","2":"Fundamental incompleto","3":"Fundamental completo",
    "4":"Médio incompleto","5":"Médio completo","6":"Superior incompleto","7":"Superior completo"
}
esc_pairs = list(chain.from_iterable([[F.lit(k), F.lit(v)] for k,v in ESC_MAP.items()]))
map_escolar = F.create_map(esc_pairs)

# perguntas dicotômicas com 1/2/3/9 conforme PNAD:
YESNO_MAP = {
    "1":"Sim",
    "2":"Não",
    "3":"Não sabe",
    "9":"Ignorado"
}
yn_pairs = list(chain.from_iterable([[F.lit(k), F.lit(v)] for k,v in YESNO_MAP.items()]))
map_yesno = F.create_map(yn_pairs)

# colunas necessárias da SOR (originais) para derivar as 20 finais
SELECT_COLS = [
    "UF","V1022","A003","A005","A002",                 # demografia/contexto
    # sintomas/saúde e atendimento
    "B0011","B0012","B0014","B00111","B0019",
    "B002","B0043","B0046","B005","B006",
    # benefícios
    "D0051","D0061","D0031"
]

# 20 finais (nomes amigáveis)
FINAL_COLS = [
    "ano","mes","uf_sigla",                # 3
    "situacao_domicilio","sexo","escolaridade","faixa_idade",  # +4 = 7
    "febre","tosse","dificuldade_respirar","perda_olfato_paladar","fadiga",  # +5 = 12
    "procurou_atendimento","atendimento_sus_hospital","atendimento_priv_hospital","internacao","sedado_entubado",  # +5 = 17
    "auxilio_emergencial","seguro_desemprego","bolsa_familia"   # +3 = 20
]

# ---------------- leitura SOR ----------------
reader = (spark.read
          .option("header", "true" if HAS_HEADER else "false")
          .option("delimiter", DELIM)
          .option("nullValue", "")
          .option("emptyValue", None)
          .option("mode", "PERMISSIVE"))

print(f"Lendo SOR de: {SOR_PATH}")
df_raw = reader.csv(SOR_PATH)

# header/no-header
if not HAS_HEADER:
    cols = df_raw.columns
    df_tmp = df_raw
    for i, c in enumerate(cols, start=1):
        df_tmp = df_tmp.withColumnRenamed(c, f"c{i}")
    missing_idx = [k for k in SELECT_COLS if k not in INDEX_MAP]
    if missing_idx and FAIL_ON_MISSING:
        raise Exception(f"INDEX_MAP faltando para colunas: {missing_idx}")
    projections = []
    for name in SELECT_COLS:
        if name in INDEX_MAP:
            projections.append(F.col(f"c{int(INDEX_MAP[name])}").alias(name))
    df = df_tmp.select(*projections) if projections else df_tmp.limit(0)
else:
    available = [c for c in SELECT_COLS if c in df_raw.columns]
    if FAIL_ON_MISSING and len(available) != len(SELECT_COLS):
        miss = [c for c in SELECT_COLS if c not in available]
        raise Exception(f"Colunas ausentes no SOR: {miss}")
    df = df_raw.select(*available)

# ---------------- extrair ano/mes do caminho ----------------
df = df.withColumn("_source", F.input_file_name())
df = df.withColumn("ano", F.regexp_extract(F.col("_source"), "ano=([0-9]{4})", 1).cast("int"))
df = df.withColumn("mes", F.regexp_extract(F.col("_source"), "mes=([0-9]{1,2})", 1).cast("int"))

if df.filter(F.col("ano").isNull() | F.col("mes").isNull()).limit(1).count() > 0:
    raise Exception(
        "Falha ao extrair ano/mes do caminho. Esperado .../ano=YYYY/mes=MM/arquivo.csv "
        "e --SOR_PATH apontando para o prefixo acima."
    )

# ---------------- derivados e rótulos ----------------
# UF -> uf_sigla
if "UF" in df.columns:
    df = df.withColumn("UF", F.col("UF").cast("string"))
    df = df.withColumn("uf_sigla", map_uf[F.col("UF")])
else:
    df = df.withColumn("uf_sigla", F.lit(None).cast("string"))

# V1022 -> situacao_domicilio
if "V1022" in df.columns:
    df = df.withColumn("situacao_domicilio", map_v1022[F.col("V1022").cast("string")])
else:
    df = df.withColumn("situacao_domicilio", F.lit(None).cast("string"))

# A003 -> sexo
if "A003" in df.columns:
    df = df.withColumn("sexo", map_sexo[F.col("A003").cast("string")])
else:
    df = df.withColumn("sexo", F.lit(None).cast("string"))

# A005 -> escolaridade
if "A005" in df.columns:
    df = df.withColumn("escolaridade", map_escolar[F.col("A005").cast("string")])
else:
    df = df.withColumn("escolaridade", F.lit(None).cast("string"))

# A002 -> faixa_idade
if "A002" in df.columns:
    df = df.withColumn("A002_idade", F.col("A002").cast("int"))
else:
    df = df.withColumn("A002_idade", F.lit(None).cast("int"))

def faixa(i):
    if i is None: return None
    try: i = int(i)
    except: return None
    if i < 15: return "0-14"
    if i < 25: return "15-24"
    if i < 40: return "25-39"
    if i < 60: return "40-59"
    return "60+"
faixa_udf = F.udf(faixa, T.StringType())
df = df.withColumn("faixa_idade", faixa_udf("A002_idade"))

# Perguntas 1/2/3/9 -> rótulos texto
def map_yn(colname, outname):
    if colname in df.columns:
        return df.withColumn(outname, map_yesno[F.col(colname).cast("string")])
    return df.withColumn(outname, F.lit(None).cast("string"))

# Sintomas
df = map_yn("B0011", "febre")
df = map_yn("B0012", "tosse")
df = map_yn("B0014", "dificuldade_respirar")
df = map_yn("B00111","perda_olfato_paladar")
df = map_yn("B0019","fadiga")

# Atendimento/gravidade
df = map_yn("B002", "procurou_atendimento")
df = map_yn("B0043","atendimento_sus_hospital")
df = map_yn("B0046","atendimento_priv_hospital")
df = map_yn("B005","internacao")
df = map_yn("B006","sedado_entubado")

# Benefícios
df = map_yn("D0051","auxilio_emergencial")
df = map_yn("D0061","seguro_desemprego")
df = map_yn("D0031","bolsa_familia")

# ---------------- selecionar EXATAMENTE 20 colunas ----------------
for c in FINAL_COLS:
    if c not in df.columns:
        df = df.withColumn(c, F.lit(None).cast("string") if c not in ["ano","mes"] else F.lit(None).cast("int"))

df_out = (df
          .select(*FINAL_COLS)
          # garanta tipos finais: ano/mes inteiros, o resto string
          .withColumn("ano", F.col("ano").cast("int"))
          .withColumn("mes", F.col("mes").cast("int"))
          )

# ---------------- escrita SOT ----------------
part_cols = ["ano","mes","uf_sigla"]
print(f"Escrevendo no SOT: {SOT_PATH}  partitionBy={part_cols}")

(df_out
    .repartition(*part_cols)
    .write
    .mode("overwrite")
    .partitionBy(*part_cols)
    .parquet(SOT_PATH)
)

print("=== FIM (SOR->SOT) OK | 20 variáveis rotuladas ===")
