import sys
import os

# ✅ Add `scripts/` directory to Python's module search path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts import get_spark_session, DB_CONFIG, DATA_PATH
from pyspark.sql.functions import lit, current_timestamp, col, trim, split, when, coalesce
from pyspark.sql.types import DateType, IntegerType, StringType,DecimalType

# ✅ Get Spark Session
spark = get_spark_session("dim_location_load")

process_id = os.getpid()

# ✅ Use Centralized Database Config
driver = DB_CONFIG["driver"]
url = DB_CONFIG["url"]
user = DB_CONFIG["user"]
password = DB_CONFIG["password"]

#write_table = "boston_311_stage"

table = "boston_311_stage"
readtable = "boston_311_stage"


DIM_TABLES = {
    "dim_date": "dim_date",
    "dim_time": "dim_time",
    "dim_location": "dim_location",
    "dim_source": "dim_source",
    "dim_request_dtl" : "dim_request_dtl"
    }

# Read required columns from `boston_311_stage`
df_stage = spark.read \
    .format("jdbc") \
    .option("driver", DB_CONFIG["driver"]) \
    .option("url", DB_CONFIG["url"]) \
    .option("dbtable", readtable) \
    .option("user", DB_CONFIG["user"]) \
    .option("password", DB_CONFIG["password"]) \
    .load()

# Extract location details and handle intersections
df_location = df_stage.select(
    "location_street_name", "neighborhood", "location_zipcode",
    "neighborhood_services_district", "police_district", "fire_district", "pwd_district",
    "city_council_district", "ward", "precinct", "latitude", "longitude"
).distinct()\
.withColumnRenamed("location_zipcode", "zipcode") \
.withColumn("neighborhood_services_district", col("neighborhood_services_district").cast(IntegerType())) \
.withColumn("police_district", col("police_district").substr(1,3)) \
.withColumn("fire_district", col("fire_district").cast(IntegerType())) \
.withColumn("pwd_district", col("pwd_district").substr(1,3)) \
.withColumn("city_council_district", col("city_council_district").cast(IntegerType())) \
.withColumn("ward", col("ward").cast(IntegerType())) \
.withColumn("precinct", col("precinct").cast(IntegerType())) \
.withColumn("latitude", col("latitude").cast(DecimalType(10,7))) \
.withColumn("longitude", col("longitude").cast(DecimalType(10,7)))

df_location = df_location.withColumn("is_intersection",
                                     when(col("location_street_name").contains("INTERSECTION"), "Y").otherwise("N")) \
                         .withColumn("street_name1",
                                     when(col("location_street_name").contains("INTERSECTION"),
                                          trim(split(col("location_street_name"), "&").getItem(0).substr(13, 100)))  # Extract part before '&'
                                     .otherwise(col("location_street_name"))) \
                         .withColumn("street_name2",
                                     when(col("location_street_name").contains("INTERSECTION"),
                                          trim(split(col("location_street_name"), "&").getItem(1)))  # Extract part after '&'
                                     .otherwise(None)) \
                         .withColumn("city", lit("Boston")) \
                         .withColumn("state", lit("MA"))
df_location = df_location.withColumn("street_name1", coalesce(col("street_name1"), lit("UNKNOWN")))


# Deduplicate data before inserting into MySQL
df_location = df_location.dropDuplicates(["street_name1", "street_name2", "neighborhood", "zipcode"])

# Add Metadata Columns
df_location = df_location.withColumn("db_created_datetime", current_timestamp()) \
                         .withColumn("db_modified_datetime", lit(None).cast("DATE")) \
                         .withColumn("created_by", lit("system")) \
                         .withColumn("modified_by", lit(None).cast("string")) \
                         .withColumn("process_id", lit(process_id))

try:
    df_location.write \
        .format("jdbc") \
           .option("driver", DB_CONFIG["driver"]) \
        .option("url", DB_CONFIG["url"]) \
        .option("dbtable", DIM_TABLES["dim_location"]) \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .option("batchsize", 1000) \
        .mode("append") \
        .save()

    print("✅ Dim Location Table Loaded!")
except Exception as e:
    print(f"❌ Error loading Dim Location Table: {e}")


# Stop the Spark session
spark.stop()
print("✅ Spark session closed.")
    