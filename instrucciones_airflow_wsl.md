# Apache Airflow con PySpark en WSL2 - Guía paso a paso

## 1. Configurar WSL2 con Ubuntu

Abrir PowerShell como Administrador:

```powershell
wsl --install -d Ubuntu
```

Si Ubuntu ya está instalado pero no es la distribución por defecto:

```powershell
wsl --set-default Ubuntu
wsl -d Ubuntu
```

## 2. Instalar dependencias del sistema

Dentro de la terminal de Ubuntu:

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install python3 python3-pip python3-venv default-jdk -y
```

Verificar que Java quedó instalado:

```bash
java -version
```

## 3. Crear entorno virtual e instalar Airflow y PySpark

```bash
python3 -m venv ~/airflow-venv
source ~/airflow-venv/bin/activate
pip install apache-airflow pyspark
```

## 4. Inicializar Airflow

```bash
airflow db migrate
```

## 5. Crear las carpetas de datos y copiar archivos

Crear las carpetas donde el pipeline lee y escribe:

```bash
sudo mkdir -p /data/raw /data/processed /data/staging
sudo chown -R $USER:$USER /data
```

Copiar el CSV de origen (los archivos de Windows están en `/mnt/d/`):

```bash
cp /mnt/d/Maestria/PythonIntroduction/AmazonSaleReport.csv /data/raw/
```

## 6. Copiar el DAG a Airflow

```bash
mkdir -p ~/airflow/dags
cp /mnt/d/Maestria/PythonIntroduction/glue_etl_amazon_sales.py ~/airflow/dags/
```

## 7. Verificar que el DAG no tiene errores

```bash
source ~/airflow-venv/bin/activate
python3 ~/airflow/dags/glue_etl_amazon_sales.py
```

Si no hay errores, el DAG está listo.

## 8. Levantar Airflow

```bash
source ~/airflow-venv/bin/activate
airflow standalone
```

Este comando hace todo en uno:
- Ejecuta las migraciones de base de datos
- Crea un usuario admin automáticamente
- Inicia el scheduler, webserver y triggerer

La contraseña del usuario `admin` se muestra en la salida del terminal.

## 9. Acceder a la UI y ejecutar el DAG

1. Abrir en el navegador de Windows: `http://localhost:8080`
2. Iniciar sesión con usuario `admin` y la contraseña del paso anterior
3. Buscar el DAG `amazon_sales_etl`
4. Activarlo y hacer click en "Trigger DAG"

Alternativamente, ejecutar por CLI en otra terminal de Ubuntu:

```bash
source ~/airflow-venv/bin/activate
airflow dags trigger amazon_sales_etl
```

## 10. Verificar resultados

El pipeline genera:
- `/data/processed/amazon_sales/detail/` - Datos limpios particionados por categoría (Parquet)
- `/data/processed/amazon_sales/summary/` - Resumen agregado (Parquet)

Para verificar que se generaron:

```bash
ls /data/processed/amazon_sales/detail/
ls /data/processed/amazon_sales/summary/
```

## Notas importantes

- **Airflow no corre en Windows nativo**, siempre debe ejecutarse dentro de WSL2.
- **Java es obligatorio** para PySpark (Spark corre sobre la JVM).
- Si se modifica el archivo del DAG en Windows, hay que volver a copiarlo a `~/airflow/dags/`.
- Para detener Airflow, presionar `Ctrl+C` en la terminal donde corre `airflow standalone`.
