"""
AWS Glue ETL Job - Amazon Sales Report
=======================================
Descripción general
-------------------
Job de AWS Glue que implementa un pipeline ETL completo sobre el dataset
AmazonSaleReport.csv. Aprovecha Apache Spark distribuido para escalar el
procesamiento a millones de registros sin cambios en el código.

Arquitectura del pipeline
--------------------------
  [S3 CSV]
      │
      ▼
  EXTRACT  ── DynamicFrame con lector vectorizado (SIMD) de Glue
      │         └─ schema inference automático
      │         └─ split por bloque S3 (paralelismo real)
      ▼
  TRANSFORM ── Spark DataFrame API
      │         └─ (a) Normalización de nombres de columnas
      │         └─ (b) Casteo de tipos (qty→int, amount→float, date→DateType)
      │         └─ (c) Deduplicación por order_id
      │         └─ (d) Filtrado de registros basura (cancelados sin monto)
      │         └─ (e) Estandarización de texto (title/upper case)
      │         └─ (f) Enriquecimiento: revenue, has_promotion, semana, flags
      │         └─ (g) Tabla de resumen agregado por categoría/mes/estado
      ▼
  LOAD ──────── DynamicFrame → S3 Parquet + Snappy
                └─ detail/   particionado por category (partition pruning)
                └─ summary/  archivo único (coalesce)

Parámetros del job (configurar en consola de Glue o CLI)
---------------------------------------------------------
  --JOB_NAME        : Nombre del job (requerido por Glue internamente)
  --S3_INPUT_PATH   : Ruta S3 del CSV origen
                      Ejemplo: s3://my-bucket/raw/AmazonSaleReport.csv
  --S3_OUTPUT_PATH  : Ruta S3 base para la salida Parquet
                      Ejemplo: s3://my-bucket/processed/amazon_sales/

Dependencias
------------
  - AWS Glue 4.0+ (PySpark 3.3, Python 3.10)
  - awsglue (incluido en el entorno Glue, no requiere instalación)
  - pyspark (incluido en el entorno Glue)

Outputs generados
-----------------
  {S3_OUTPUT_PATH}/detail/category=<valor>/   → registros limpios y enriquecidos
  {S3_OUTPUT_PATH}/summary/                   → métricas agregadas por categoría

Notas de escalabilidad
-----------------------
  - El job bookmark (job.commit) permite ejecución incremental: en re-ejecuciones
    solo procesa archivos S3 nuevos, evitando reprocesar datos ya cargados.
  - optimizePerformance=True activa el lector SIMD vectorizado de Glue, que puede
    duplicar la velocidad de lectura en archivos CSV grandes.
  - El particionado por 'category' en la salida reduce el escaneo en queries de
    Athena o Redshift Spectrum al filtrar por esa columna.
"""

import sys

# awsglue: librería propia de AWS Glue que envuelve PySpark con utilidades ETL
from awsglue.transforms import *          # transformaciones nativas de Glue (ResolveChoice, etc.)
from awsglue.utils import getResolvedOptions  # lee parámetros pasados al job
from awsglue.context import GlueContext   # punto de entrada principal de Glue
from awsglue.job import Job               # gestiona el ciclo de vida del job y bookmarks
from awsglue.dynamicframe import DynamicFrame  # estructura de datos de Glue (schema flexible)

# pyspark: API de Apache Spark para procesamiento distribuido
from pyspark.context import SparkContext
from pyspark.sql import functions as F    # funciones SQL vectorizadas (col, round, year, etc.)
from pyspark.sql.types import (
    DoubleType, IntegerType, DateType, BooleanType, StringType
)

# ---------------------------------------------------------------------------
# 1. Inicialización del contexto de Glue y Spark
#
#    SparkContext  → motor de ejecución distribuida en el cluster de workers
#    GlueContext   → capa de abstracción de Glue sobre SparkContext; provee
#                    métodos para leer/escribir DynamicFrames desde/hacia S3,
#                    catálogo de Glue, JDBC, etc.
#    spark_session → acceso a la API de DataFrame de Spark (SQL, funciones, etc.)
#    Job           → registra el inicio del job y habilita los bookmarks
# ---------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "S3_INPUT_PATH", "S3_OUTPUT_PATH"],
)

sc = SparkContext()                        # un SparkContext por JVM (singleton)
glueContext = GlueContext(sc)
spark = glueContext.spark_session          # SparkSession para DataFrame API
job = Job(glueContext)
job.init(args["JOB_NAME"], args)           # registra inicio; activa job bookmark

# ---------------------------------------------------------------------------
# 2. EXTRACT — Lectura del CSV con DynamicFrame
#
#    create_dynamic_frame.from_options lee el CSV desde S3 de forma distribuida:
#      - Glue divide el archivo en splits (bloques S3) y los asigna a distintos
#        workers en paralelo → escalamiento horizontal automático.
#      - recurse=True permite leer carpetas con múltiples archivos CSV.
#      - optimizePerformance=True activa el lector SIMD/vectorizado de Glue,
#        que procesa columnas en lote en lugar de fila a fila (hasta 2x más rápido).
#      - transformation_ctx es un identificador único usado por el job bookmark
#        para rastrear qué datos ya fueron procesados en ejecuciones anteriores.
#
#    DynamicFrame vs DataFrame:
#      DynamicFrame tolera schemas inconsistentes (campos nulos, tipos mixtos);
#      ideal para datos crudos. Se convierte a DataFrame para transformaciones.
# ---------------------------------------------------------------------------
raw_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args["S3_INPUT_PATH"]], "recurse": True},
    format="parquet",
    transformation_ctx="raw_dyf",
)

print(f"[EXTRACT] Schema original:")
raw_dyf.printSchema()
print(f"[EXTRACT] Registros leídos: {raw_dyf.count()}")

# ---------------------------------------------------------------------------
# 3. TRANSFORM — Trabajamos con Spark DataFrame para aprovechar la API completa
#
#    Se convierte el DynamicFrame a DataFrame de Spark porque:
#      - La API de DataFrame ofrece más funciones (withColumn, filter, groupBy, etc.)
#      - Las operaciones son lazy (evaluación perezosa): Spark construye un plan
#        de ejecución optimizado (Catalyst) y solo ejecuta cuando se llama count()
#        o una acción de escritura → eficiencia en el cluster.
# ---------------------------------------------------------------------------
df = raw_dyf.toDF()  # convierte DynamicFrame → Spark DataFrame

# 3-a. Normalizar nombres de columnas
#      (elimina espacios, guiones y caracteres problemáticos)
rename_map = {
    "Order ID":              "order_id",
    "Date":                  "order_date",
    "Status":                "status",
    "Fulfilment":            "fulfilment",
    "Sales Channel ":        "sales_channel",   # nota el espacio trailing
    "ship-service-level":    "ship_service_level",
    "Style":                 "style",
    "SKU":                   "sku",
    "Category":              "category",
    "Size":                  "size",
    "ASIN":                  "asin",
    "Courier Status":        "courier_status",
    "Qty":                   "qty",
    "currency":              "currency",
    "Amount":                "amount",
    "ship-city":             "ship_city",
    "ship-state":            "ship_state",
    "ship-postal-code":      "ship_postal_code",
    "ship-country":          "ship_country",
    "promotion-ids":         "promotion_ids",
    "B2B":                   "is_b2b",
    "fulfilled-by":          "fulfilled_by",
    "Unnamed: 22":           "extra_col",
    "index":                 "row_index",
}

for old_name, new_name in rename_map.items():
    if old_name in df.columns:
        df = df.withColumnRenamed(old_name, new_name)

# 3-b. Casteo de tipos
#      El CSV original tiene todo como String. Se castea cada columna al tipo
#      semántico correcto para habilitar operaciones numéricas y de fecha.
#      Formato de fecha original en el CSV: "04-30-22" (MM-dd-yy).
df = (
    df
    .withColumn("qty",    F.col("qty").cast(IntegerType()))
    .withColumn("amount", F.col("amount").cast(DoubleType()))
    .withColumn("is_b2b", F.col("is_b2b").cast(BooleanType()))
    .withColumn(
        "order_date",
        F.to_date(F.col("order_date"), "MM-dd-yy"),   # "04-30-22" → 2022-04-30
    )
    .withColumn("ship_postal_code", F.col("ship_postal_code").cast(StringType()))
)

# 3-c. Eliminar columna basura y deduplicar
#      'extra_col' (Unnamed: 22) y 'row_index' no aportan valor analítico.
#      dropDuplicates usa hashing distribuido en Spark: cada worker deduplica
#      su partición local y luego se hace un shuffle para deduplicación global.
df = df.drop("extra_col", "row_index")
df = df.dropDuplicates(["order_id"])

# 3-d. Filtrar pedidos cancelados con qty 0 y sin monto
#      (se conservan cancelados con monto para análisis de pérdida)
df = df.filter(
    ~((F.col("status") == "Cancelled") & (F.col("qty") == 0) & (F.col("amount") == 0))
)

# 3-e. Estandarizar campos de texto
#      initcap → primera letra mayúscula por palabra (ej: "kurta" → "Kurta")
#      upper   → todo en mayúsculas para ciudad/estado (normaliza variantes)
#      trim    → elimina espacios al inicio/fin (frecuente en CSVs exportados)
df = (
    df
    .withColumn("status",       F.initcap(F.col("status")))
    .withColumn("category",     F.initcap(F.trim(F.col("category"))))
    .withColumn("ship_city",    F.upper(F.trim(F.col("ship_city"))))
    .withColumn("ship_state",   F.upper(F.trim(F.col("ship_state"))))
    .withColumn("sales_channel",F.trim(F.col("sales_channel")))
)

# 3-f. Columnas derivadas / enriquecimiento
#      Nuevas columnas calculadas que agregan valor analítico sin modificar los datos originales:
#        revenue          → ingreso real del pedido (qty × precio unitario)
#        has_promotion    → indica si el pedido usó algún código de promoción
#        order_year/month → extraídos de order_date para facilitar agrupaciones temporales
#        order_week       → número ISO de semana para análisis de estacionalidad semanal
#        is_express       → True si el envío fue con nivel "Expedited" (más rápido y costoso)
#        fulfilled_amazon → True si Amazon manejó la logística (vs. el vendedor directamente)
df = (
    df
    .withColumn("revenue",         F.round(F.col("qty") * F.col("amount"), 2))
    .withColumn("has_promotion",   F.col("promotion_ids").isNotNull() & (F.col("promotion_ids") != ""))
    .withColumn("order_year",      F.year(F.col("order_date")))
    .withColumn("order_month",     F.month(F.col("order_date")))
    .withColumn("order_week",      F.weekofyear(F.col("order_date")))
    .withColumn("is_express",      F.col("ship_service_level") == "Expedited")
    .withColumn("fulfilled_amazon",F.col("fulfilment") == "Amazon")
)

# 3-g. Tabla de resumen agregado por categoría, mes y estado
#      groupBy en Spark es un shuffle distribuido: cada worker agrupa sus particiones
#      locales (map) y luego envía los datos al worker responsable de cada clave (reduce).
#      Métricas generadas por grupo:
#        total_orders      → conteo de pedidos únicos
#        total_units       → unidades vendidas
#        total_revenue     → ingresos totales (suma de revenue)
#        avg_unit_price    → precio promedio unitario
#        unique_skus       → variedad de productos distintos
#        orders_with_promo → pedidos que usaron promociones
#        b2b_orders        → pedidos de empresa a empresa
summary_df = (
    df
    .groupBy("category", "order_year", "order_month", "ship_state")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("qty").alias("total_units"),
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.round(F.avg("amount"), 2).alias("avg_unit_price"),
        F.countDistinct("sku").alias("unique_skus"),
        F.sum(F.col("has_promotion").cast(IntegerType())).alias("orders_with_promo"),
        F.sum(F.col("is_b2b").cast(IntegerType())).alias("b2b_orders"),
    )
    .orderBy("order_year", "order_month", "category")
)

print("[TRANSFORM] Schema transformado:")
df.printSchema()
print(f"[TRANSFORM] Registros tras limpieza: {df.count()}")
print("[TRANSFORM] Muestra de resumen agregado:")
summary_df.show(10, truncate=False)

# ---------------------------------------------------------------------------
# 4. LOAD — Escritura en S3 como Parquet optimizado
#    - Parquet columnar es ideal para análisis en Athena / Redshift Spectrum
#    - Particionado por categoría mejora el pruning en queries posteriores
#    - coalesce evita archivos pequeños ("small files problem")
# ---------------------------------------------------------------------------

# 4-a. Datos detallados particionados por category
#      Se convierte el DataFrame de vuelta a DynamicFrame para usar el escritor
#      nativo de Glue (glueparquet), que gestiona el registro automático en el
#      Catálogo de Glue y aplica optimizaciones de escritura en S3.
#      partitionKeys=["category"] crea subcarpetas category=<valor>/ en S3,
#      lo que permite a Athena/Redshift Spectrum saltar particiones completas
#      al filtrar por esa columna (partition pruning → menos datos leídos = menor costo).
detail_dyf = DynamicFrame.fromDF(df, glueContext, "detail_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=detail_dyf,
    connection_type="s3",
    connection_options={
        "path": args["S3_OUTPUT_PATH"] + "detail/",
        "partitionKeys": ["category"],
    },
    format="glueparquet",
    format_options={"compression": "snappy"},
    transformation_ctx="write_detail",
)

# 4-b. Tabla de resumen (un solo archivo, sin partición)
#      coalesce(1) reduce las particiones Spark a 1 antes de escribir,
#      produciendo un único archivo Parquet. Adecuado para tablas de resumen
#      pequeñas donde un solo archivo es más práctico para descargar o consultar.
summary_df_coalesced = summary_df.coalesce(1)
summary_dyf = DynamicFrame.fromDF(summary_df_coalesced, glueContext, "summary_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=summary_dyf,
    connection_type="s3",
    connection_options={"path": args["S3_OUTPUT_PATH"] + "summary/"},
    format="glueparquet",
    format_options={"compression": "snappy"},
    transformation_ctx="write_summary",
)

print(f"[LOAD] Datos detallados escritos en: {args['S3_OUTPUT_PATH']}detail/")
print(f"[LOAD] Resumen escrito en:           {args['S3_OUTPUT_PATH']}summary/")

# ---------------------------------------------------------------------------
# 5. Commit del job — habilita el job bookmark para procesamiento incremental
#
#    job.commit() marca el punto hasta donde llegó esta ejecución.
#    En la próxima ejecución del job, Glue leerá desde S3 solo los archivos
#    que fueron agregados después de este commit, evitando reprocesar datos.
#    Si se omite este llamado, el bookmark no avanza y se reprocesaría todo
#    el dataset en cada ejecución.
# ---------------------------------------------------------------------------
job.commit()
print("[DONE] Job completado exitosamente.")
