import sys
import os

# ✅ Add `scripts/` directory to Python's module search path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts import get_spark_session, DB_CONFIG  # Removed `DATA_PATH`
from pyspark.sql.functions import lit, current_timestamp, col, trim, upper, substring

# ✅ Correct DATA_PATH for WSL
DATA_PATH = "/home/amey379/airflow/files/boston_311"

# ✅ Get Spark Session
spark = get_spark_session("stage_load")

# ✅ Use Centralized Database Config
driver = DB_CONFIG["driver"]
url = DB_CONFIG["url"]
user = DB_CONFIG["user"]
password = DB_CONFIG["password"]
write_table = "boston_311_stage"

print("✅ Connecting to DB:", url)  # Debugging log

# ✅ Load and Transform CSV Files
def transform_and_load(df_boston_311):
    df_boston_311 = df_boston_311.selectExpr(
        "case_enquiry_id as case_enquiry_id",
        "CAST(open_dt AS timestamp) as open_dt",
        "CAST(sla_target_dt AS timestamp) as sla_target_dt",
        "CAST(closed_dt AS timestamp) as closed_dt",
        "on_time",
        "case_status",
        "closure_reason",
        "case_title",
        "subject",
        "reason",
        "type",
        "queue",
        "department",
        "location",
        "fire_district",
        "pwd_district",
        "city_council_district",
        "police_district",
        "neighborhood",
        "neighborhood_services_district",
        "ward",
        "precinct",
        "location_street_name",
        "format_string('%05d', location_zipcode) as location_zipcode",
        "CAST(latitude AS DECIMAL(10, 7)) as latitude",
        "CAST(longitude AS DECIMAL(10, 7)) as longitude",
        "geom_4326",
        "source"
    )

    # ✅ Add Additional Columns
    df_boston_311 = df_boston_311.withColumn("db_created_datetime", current_timestamp()) \
                                 .withColumn("closure_reason", substring("closure_reason", 1, 255)) \
                                 .withColumn("db_modified_datetime", lit(None).cast("DATE")) \
                                 .withColumn("created_by", lit("system")) \
                                 .withColumn("modified_by", lit("system")) \
                                 .withColumn("process_id", lit(1))

    # ✅ Trim and Convert String Columns to Uppercase
    string_columns = [field.name for field in df_boston_311.schema.fields if str(field.dataType) == "StringType"]
    columns_to_upper = ["on_time", "case_status", "case_title", "subject", "reason", "type", "location", "neighborhood", "location_street_name", "source"]

    for col_name in string_columns:
        df_boston_311 = df_boston_311.withColumn(col_name, trim(col(col_name)))

    for col_name in columns_to_upper:
        df_boston_311 = df_boston_311.withColumn(col_name, upper(trim(col(col_name))))

    try:
        df_boston_311.write \
            .format("jdbc") \
            .option("driver", driver) \
            .option("url", url) \
            .option("dbtable", write_table) \
            .option("user", user) \
            .option("password", password) \
            .option("batchsize", 1000) \
            .mode("append") \
            .save()
        print("✅ Data written successfully to MySQL!")
    except Exception as e:
        print(f"❌ Error writing to MySQL: {e}")

# ✅ Loop through CSV files
for i in range(2015, 2025):
    csv_path = f"{DATA_PATH}/Boston_311_{i}.csv"

    if os.path.exists(csv_path):
        print(f"🔎 Processing file: {csv_path}")  # Debugging
        df_boston_311_req = spark.read.csv(csv_path, header=True, inferSchema=True)
        transform_and_load(df_boston_311_req)
        print(f"✅ Processed: {csv_path}")
    else:
        print(f"❌ File not found: {csv_path}")  # Debugging

# ✅ Stop the Spark Session
spark.stop()
print("✅ Spark session closed.")