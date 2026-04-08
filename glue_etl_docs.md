# glue_etl_amazon_sales.py — ¿Qué hace este script?

## Idea general

Este script lee un archivo con ventas de Amazon, lo limpia y organiza,
y guarda el resultado en un formato más eficiente llamado Parquet.

Todo esto lo hace usando **AWS Glue**, un servicio de Amazon que permite
procesar archivos muy grandes porque divide el trabajo entre varias
computadoras al mismo tiempo.

---

## ¿Qué es ETL?

ETL son las siglas de **Extract, Transform, Load** (Extraer, Transformar, Cargar).
Es el proceso estándar para mover y limpiar datos:

```
EXTRACT  → Leer el archivo de entrada
TRANSFORM → Limpiar y organizar los datos
LOAD     → Guardar el resultado listo para análisis
```

---

## ¿Qué hace cada parte?

### 1. EXTRACT — Leer el archivo

Se lee el archivo `AmazonSaleReport.parquet` desde Amazon S3 (el almacenamiento
en la nube de AWS). AWS Glue divide el archivo en pedazos y los lee al mismo
tiempo para ir más rápido.

> **Nota:** El archivo de entrada es Parquet y no CSV. El CSV original se convierte
> a Parquet localmente antes de subirlo a S3 con este comando:
> ```bash
> python -c "import pandas as pd; pd.read_csv('AmazonSaleReport.csv').to_parquet('AmazonSaleReport.parquet', index=False)"
> aws s3 cp AmazonSaleReport.parquet s3://python-introduction/raw/AmazonSaleReport.parquet
> ```

### 2. TRANSFORM — Limpiar y organizar

Se aplican varias limpiezas sobre los datos:

| Paso | Qué hace                         | Ejemplo                                         |
|------|----------------------------------|-------------------------------------------------|
| a    | Renombrar columnas               | `"Order ID"` → `order_id`                       |
| b    | Corregir tipos de datos          | La cantidad pasa de texto a número entero        |
| c    | Eliminar pedidos duplicados      | Si un pedido aparece dos veces, se queda uno     |
| d    | Eliminar filas vacías            | Cancelados con cantidad 0 y monto 0 se eliminan  |
| e    | Uniformar el texto               | `"kurta"`, `"KURTA"` y `"Kurta"` quedan iguales |
| f    | Crear columnas nuevas            | Se calcula el ingreso total por pedido           |
| g    | Crear un resumen por categoría   | Total de ventas, unidades y promedio por mes     |

### 3. LOAD — Guardar los resultados

Se guardan dos salidas en formato Parquet dentro de S3:

- **detail/** → todos los pedidos limpios, organizados por categoría de producto
- **summary/** → un resumen con totales por categoría, mes y estado

---

## Columnas nuevas que se crean

| Columna            | Qué representa                                       |
|--------------------|------------------------------------------------------|
| `revenue`          | Ingreso del pedido (cantidad × precio)               |
| `has_promotion`    | Si el pedido usó algún descuento o promoción         |
| `order_year`       | Año del pedido                                       |
| `order_month`      | Mes del pedido                                       |
| `order_week`       | Semana del año en que se hizo el pedido              |
| `is_express`       | Si el envío fue exprés (más rápido)                  |
| `fulfilled_amazon` | Si Amazon se encargó del envío                       |

---

## Output generado en S3

Después de ejecutar el job, los datos quedan organizados así en el bucket `python-introduction`:

```
s3://python-introduction/
├── raw/
│   └── AmazonSaleReport.parquet       ← archivo de entrada
└── processed/
    └── amazon_sales/
        ├── detail/
        │   ├── category=Kurta/        ← pedidos de Kurta limpios y enriquecidos
        │   ├── category=Set/          ← pedidos de Set limpios y enriquecidos
        │   ├── category=Western Dress/
        │   ├── category=Ethnic Dress/
        │   ├── category=Blouse/
        │   ├── category=Bottom/
        │   ├── category=Saree/
        │   └── ...
        └── summary/
            └── part-00000.parquet     ← tabla con totales por categoría y mes
```

### ¿Qué contiene cada salida?

**detail/** — Un archivo por cada categoría de producto con todos los pedidos limpios.
Cada archivo tiene estas columnas:

| Columna             | Qué es                                      |
|---------------------|---------------------------------------------|
| `order_id`          | Identificador único del pedido              |
| `order_date`        | Fecha del pedido                            |
| `status`            | Estado (Shipped, Cancelled, etc.)           |
| `category`          | Categoría del producto                      |
| `qty`               | Cantidad vendida                            |
| `amount`            | Precio unitario en rupias (INR)             |
| `ship_city`         | Ciudad de destino                           |
| `ship_state`        | Estado de destino                           |
| `is_b2b`            | Si fue una venta a empresa                  |
| `revenue`           | Ingreso total del pedido                    |
| `has_promotion`     | Si usó promoción                            |
| `order_year`        | Año del pedido                              |
| `order_month`       | Mes del pedido                              |
| `order_week`        | Semana del año                              |
| `is_express`        | Si el envío fue exprés                      |
| `fulfilled_amazon`  | Si Amazon gestionó el envío                 |

**summary/** — Una sola tabla con las ventas agrupadas por categoría, mes y estado:

| Columna              | Qué es                                     |
|----------------------|--------------------------------------------|
| `category`           | Categoría del producto                     |
| `order_year`         | Año                                        |
| `order_month`        | Mes                                        |
| `ship_state`         | Estado de destino                          |
| `total_orders`       | Cantidad de pedidos                        |
| `total_units`        | Unidades vendidas                          |
| `total_revenue`      | Ingresos totales                           |
| `avg_unit_price`     | Precio promedio por unidad                 |
| `unique_skus`        | Cantidad de productos distintos vendidos   |
| `orders_with_promo`  | Pedidos que usaron promoción               |
| `b2b_orders`         | Pedidos de empresa a empresa               |

---

## ¿Por qué usar AWS Glue y no simplemente Python?

Con un archivo pequeño, Python normal funciona bien. Pero si el archivo
tuviera millones de filas (por ejemplo, todas las ventas del año), una sola
computadora tardaría mucho tiempo o se quedaría sin memoria.

AWS Glue divide el trabajo entre varias máquinas en la nube y las hace
trabajar al mismo tiempo, por eso puede manejar archivos enormes sin problema.

---

## Parámetros que necesita el script

| Parámetro            | Qué es                               | Valor usado                                              |
|----------------------|--------------------------------------|----------------------------------------------------------|
| `--JOB_NAME`         | Nombre que le damos al proceso       | `amazon-sales-etl`                                       |
| `--S3_INPUT_PATH`    | Dónde está el archivo en la nube     | `s3://python-introduction/raw/AmazonSaleReport.parquet`  |
| `--S3_OUTPUT_PATH`   | Dónde guardar los resultados         | `s3://python-introduction/processed/amazon_sales/`       |
