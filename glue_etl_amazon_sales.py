"""
Apache Airflow DAG - Amazon Sales Report ETL
=============================================
Descripción general
-------------------
DAG de Apache Airflow que implementa un pipeline ETL completo sobre el dataset
AmazonSaleReport.csv. Usa PySpark en modo local para procesamiento distribuido
en todos los cores disponibles de la máquina.

Arquitectura del pipeline
--------------------------
  [CSV local]
      │
      ▼
  EXTRACT  ── SparkSession local, lectura CSV
      │         └─ schema inference automático
      │         └─ normalización de columnas + casteo de tipos
      │         └─ escribe Parquet intermedio
      ▼
  TRANSFORM ── Spark DataFrame API
      │         └─ (a) Deduplicación por order_id
      │         └─ (b) Filtrado de registros basura (cancelados sin monto)
      │         └─ (c) Estandarización de texto (title/upper case)
      │         └─ (d) Enriquecimiento: revenue, has_promotion, semana, flags
      │         └─ (e) Tabla de resumen agregado por categoría/mes/estado
      │         └─ escribe detail/ y summary/ como Parquet
      ▼
  LOAD_VERIFY ── Lee Parquet de salida y verifica conteos

Configuración
--------------
  Ajustar las constantes INPUT_PATH, OUTPUT_PATH e INTERMEDIATE_PATH
  según el entorno donde se ejecute Airflow.

Dependencias
------------
  - Apache Airflow 2.x+
  - PySpark 3.x
  - Python 3.9+

Outputs generados
-----------------
  {OUTPUT_PATH}/detail/category=<valor>/   → registros limpios y enriquecidos
  {OUTPUT_PATH}/summary/                   → métricas agregadas por categoría
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, DateType, BooleanType, StringType
)

# ---------------------------------------------------------------------------
# Configuración de rutas (ajustar según el entorno)
# ---------------------------------------------------------------------------
INPUT_PATH = "/data/raw/AmazonSaleReport.csv"
OUTPUT_PATH = "/data/processed/amazon_sales"
INTERMEDIATE_PATH = "/data/staging/amazon_sales_clean"

# Mapa de renombrado de columnas del CSV original
RENAME_MAP = {
    "Order ID":              "order_id",
    "Date":                  "order_date",
    "Status":                "status",
    "Fulfilment":            "fulfilment",
    "Sales Channel ":        "sales_channel",
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


# ---------------------------------------------------------------------------
# EXTRACT — Lectura del CSV, renombrado de columnas y casteo de tipos.
#           Escribe un Parquet intermedio para desacoplar del paso Transform.
# ---------------------------------------------------------------------------
def extract(**kwargs):
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("amazon_sales_extract") \
        .getOrCreate()

    try:
        # Lectura del CSV con inferencia de schema
        df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)

        print(f"[EXTRACT] Registros leídos: {df.count()}")

        # Normalizar nombres de columnas
        for old_name, new_name in RENAME_MAP.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)

        # Casteo de tipos
        df = (
            df
            .withColumn("qty",    F.col("qty").cast(IntegerType()))
            .withColumn("amount", F.col("amount").cast(DoubleType()))
            .withColumn("is_b2b", F.col("is_b2b").cast(BooleanType()))
            .withColumn(
                "order_date",
                F.to_date(F.col("order_date"), "MM-dd-yy"),
            )
            .withColumn("ship_postal_code", F.col("ship_postal_code").cast(StringType()))
        )

        # Escribir Parquet intermedio
        df.write.mode("overwrite").parquet(INTERMEDIATE_PATH)
        print(f"[EXTRACT] Parquet intermedio escrito en: {INTERMEDIATE_PATH}")

    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# TRANSFORM — Limpieza, enriquecimiento y agregación.
#             Lee el Parquet intermedio y escribe detail/ y summary/.
# ---------------------------------------------------------------------------
def transform(**kwargs):
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("amazon_sales_transform") \
        .getOrCreate()

    try:
        df = spark.read.parquet(INTERMEDIATE_PATH)

        # Eliminar columnas basura y deduplicar
        df = df.drop("extra_col", "row_index")
        df = df.dropDuplicates(["order_id"])

        # Filtrar pedidos cancelados con qty 0 y sin monto
        df = df.filter(
            ~(
                (F.col("status") == "Cancelled")
                & (F.col("qty") == 0)
                & (F.col("amount") == 0)
            )
        )

        # Estandarizar campos de texto
        df = (
            df
            .withColumn("status",        F.initcap(F.col("status")))
            .withColumn("category",      F.initcap(F.trim(F.col("category"))))
            .withColumn("ship_city",     F.upper(F.trim(F.col("ship_city"))))
            .withColumn("ship_state",    F.upper(F.trim(F.col("ship_state"))))
            .withColumn("sales_channel", F.trim(F.col("sales_channel")))
        )

        # Columnas derivadas / enriquecimiento
        df = (
            df
            .withColumn("revenue",          F.round(F.col("qty") * F.col("amount"), 2))
            .withColumn("has_promotion",    F.col("promotion_ids").isNotNull() & (F.col("promotion_ids") != ""))
            .withColumn("order_year",       F.year(F.col("order_date")))
            .withColumn("order_month",      F.month(F.col("order_date")))
            .withColumn("order_week",       F.weekofyear(F.col("order_date")))
            .withColumn("is_express",       F.col("ship_service_level") == "Expedited")
            .withColumn("fulfilled_amazon", F.col("fulfilment") == "Amazon")
        )

        # Resumen agregado por categoría, mes y estado
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

        print(f"[TRANSFORM] Registros tras limpieza: {df.count()}")

        # Escribir datos detallados particionados por category
        df.write.mode("overwrite").partitionBy("category").parquet(f"{OUTPUT_PATH}/detail/")

        # Escribir resumen (un solo archivo)
        summary_df.coalesce(1).write.mode("overwrite").parquet(f"{OUTPUT_PATH}/summary/")

        print(f"[TRANSFORM] Detalle escrito en: {OUTPUT_PATH}/detail/")
        print(f"[TRANSFORM] Resumen escrito en: {OUTPUT_PATH}/summary/")

    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# LOAD_VERIFY — Lee los Parquet de salida y verifica conteos y muestras.
# ---------------------------------------------------------------------------
def load_verify(**kwargs):
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("amazon_sales_verify") \
        .getOrCreate()

    try:
        detail_df = spark.read.parquet(f"{OUTPUT_PATH}/detail/")
        summary_df = spark.read.parquet(f"{OUTPUT_PATH}/summary/")

        detail_count = detail_df.count()
        summary_count = summary_df.count()

        print(f"[VERIFY] Registros en detail: {detail_count}")
        print(f"[VERIFY] Registros en summary: {summary_count}")

        if detail_count == 0:
            raise ValueError("El dataset detail está vacío — posible error en el pipeline.")

        print("[VERIFY] Schema detail:")
        detail_df.printSchema()

        print("[VERIFY] Muestra de resumen:")
        summary_df.show(10, truncate=False)

        print("[VERIFY] Pipeline completado exitosamente.")

    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# Definición del DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="amazon_sales_etl",
    description="ETL pipeline para Amazon Sales Report: CSV → Parquet con PySpark",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "amazon", "pyspark"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_verify_task = PythonOperator(
        task_id="load_verify",
        python_callable=load_verify,
    )

    extract_task >> transform_task >> load_verify_task
