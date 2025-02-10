import findspark
import pyspark
from pyspark.sql import SparkSession
import logging
import os



# ✅ Initialize Logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# ✅ Initialize Spark Session
def get_spark_session(app_name="ETL_Pipeline"):
    return SparkSession.builder \
        .master('local[*]') \
        .appName(app_name) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .getOrCreate()
#        .config("spark.driver.extraClassPath", "/home/amey379/spark/jars/mysql-connector-java-8.0.33.jar") \
# ✅ Database Configuration
DB_CONFIG = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "host": "172.21.0.1",
    "port": "3306",
    "database": "requests_311",
    "user": "root",
    "password": "amey@1105",
    "url": "jdbc:mysql://172.21.0.1:3306/requests_311"
}

# ✅ Data File Path
DATA_PATH = "F:/GCP project/"

logging.info("Airflow ETL scripts are running in LOCAL mode.")
logging.info(f"Data Path: {DATA_PATH}")
