import sys
import os


# ✅ Add `scripts/` directory to Python's module search path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts import get_spark_session, DB_CONFIG, DATA_PATH
from pyspark.sql.functions import lit, current_timestamp, col, trim, upper, substring, split, explode, when, regexp_extract


# ✅ Get Spark Session
spark = get_spark_session("lookup_load")

process_id = os.getpid()

# ✅ Use Centralized Database Config
driver = DB_CONFIG["driver"]
url = DB_CONFIG["url"]
user = DB_CONFIG["user"]
password = DB_CONFIG["password"]

#write_table = "boston_311_stage"

table = "boston_311_stage"
readtable = "boston_311_stage"


LOOKUP_TABLES = {
    "police_districts": "lkp_police_districts",
    "fire_districts": "lkp_fire_districts",
    "pwd_districts": "lkp_pwd_districts",
    "city_council_districts": "lkp_city_council_districts",
    "electoral_div": "lkp_electoral_divisions"
}


data = [
    ("A1", "Downtown"),
    ("A15", "Charlestown"),
    ("A7", "East Boston"),
    ("B2", "Roxbury"),
    ("B3", "Mattapan"),
    ("C6", "South Boston"),
    ("C11", "Dorchester"),
    ("D4", "South End"),
    ("D14", "Brighton"),
    ("E5", "West Roxbury"),
    ("E13", "Jamaica Plain"),
    ("E18", "Hyde Park")
]

columns = ["police_district", "name"]
df_police_districts = spark.createDataFrame(data, columns)


df_police_districts = df_police_districts.withColumn("db_created_datetime", current_timestamp()) \
                                 .withColumn("db_modified_datetime", lit(None).cast("DATE")) \
                                 .withColumn("created_by", lit("system")) \
                                 .withColumn("modified_by", lit(None).cast("string")) \
                                 .withColumn("process_id", lit(process_id))

try:
    df_police_districts.write \
        .format("jdbc") \
        .option("driver", DB_CONFIG["driver"]) \
        .option("url", DB_CONFIG["url"]) \
        .option("dbtable", LOOKUP_TABLES["police_districts"]) \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .mode("overwrite") \
        .save()
    print("✅ Police Districts Loaded!")
except Exception as e:
    print(f"❌ Error loading Police Districts: {e}")



# Define the fire district data
data1 = [
    (1, "Division 1", "East Boston"),
    (3, "Division 1", "Beacon Hill, West End, North End, Charlestown"),
    (4, "Division 1", "Back Bay, South End"),
    (6, "Division 1", "South Boston, Downtown"),
    (11, "Division 1", "Allston, Brighton"),
    (7, "Division 2", "Dorchester, Roxbury"),
    (8, "Division 2", "Dorchester, Boston Harbor (Moon Island, Long Island)"),
    (9, "Division 2", "Roxbury, Jamaica Plain"),
    (12, "Division 2", "Roslindale, Hyde Park, West Roxbury")
]

columns = ["fire_district", "division", "name"]

# Create DataFrame
df_fire_districts = spark.createDataFrame(data1, columns)


df_fire_districts = df_fire_districts.withColumn("db_created_datetime", current_timestamp()) \
                                 .withColumn("db_modified_datetime", lit(None).cast("DATE")) \
                                 .withColumn("created_by", lit("system")) \
                                 .withColumn("modified_by", lit(None).cast("string")) \
                                 .withColumn("process_id", lit(process_id))
try:
    df_fire_districts.write \
        .format("jdbc") \
        .option("driver", DB_CONFIG["driver"]) \
        .option("url", DB_CONFIG["url"]) \
        .option("dbtable", LOOKUP_TABLES["fire_districts"]) \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .mode("overwrite") \
        .save()
    print("✅ Fire Districts Loaded!")
except Exception as e:
    print(f"❌ Error loading Fire Districts: {e}")


data = [
    ("West Roxbury/Roslindale", "06"),
    ("East Boston", "09"),
    ("Charlestown", "1A"),
    ("Beacon Hill/West End/North End", "1B"),
    ("Allston/Brighton", "04"),
    ("South Boston", "05"),
    ("Back Bay/South End/Downtown", "1C"),
    ("Roxbury", "10B"),
    ("Kenmore/Fenway/Mission Hill", "10A"),
    ("North Dorchester", "03"),
    ("South Dorchester", "07"),
    ("Hyde Park", "08"),
    ("Jamaica Plain/Roslindale", "02")
]

# Define columns
columns = ["name", "pwd_district"]

# Create DataFrame
df_pwd_districts = spark.createDataFrame(data, columns)


# Add metadata columns for tracking
df_pwd_districts = df_pwd_districts.withColumn("db_created_datetime", current_timestamp()) \
                                   .withColumn("db_modified_datetime", lit(None).cast("timestamp")) \
                                   .withColumn("created_by", lit("system")) \
                                   .withColumn("modified_by", lit(None).cast("string")) \
                                   .withColumn("process_id", lit(process_id))


try:
    df_pwd_districts.write \
        .format("jdbc") \
        .option("driver", DB_CONFIG["driver"]) \
        .option("url", DB_CONFIG["url"]) \
        .option("dbtable", LOOKUP_TABLES["pwd_districts"]) \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .mode("overwrite") \
        .save()
    print("✅ PWD Districts Loaded!")
except Exception as e:
    print(f"❌ Error loading PWD Districts: {e}")



data = [
    (1, "CHARLESTOWN, EAST BOSTON, NORTH END", "SAL LAMATTINA"),
    (2, "DOWNTOWN, SOUTH BOSTON, SOUTH END", "BILL LINEHAN"),
    (3, "DORCHESTER", "FRANK BAKER"),
    (4, "MATTAPAN", "CHARLES YANCEY"),
    (5, "HYDE PARK, ROSLINDALE", "TIMOTHY MCCARTHY"),
    (6, "JAMAICA PLAIN, WEST ROXBURY", "MATT O’MALLEY"),
    (7, "ROXBURY", "TITO JACKSON"),
    (8, "BACK BAY, BEACON HILL, FENWAY / KENMORE, MISSION HILL, WEST END", "JOSH ZAKIM"),
    (9, "ALLSTON / BRIGHTON", "MARK CIOMMO")
]


# Define columns
columns = ["city_council_district", "name", "councilor"]

# Create DataFrame
df_city_council_districts = spark.createDataFrame(data, columns)


# Add metadata columns for tracking
df_city_council_districts = df_city_council_districts.withColumn("db_created_datetime", current_timestamp()) \
                                                     .withColumn("db_modified_datetime", lit(None).cast("timestamp")) \
                                                     .withColumn("created_by", lit("system")) \
                                                     .withColumn("modified_by", lit(None).cast("string")) \
                                                     .withColumn("process_id", lit(process_id))



try:
    df_city_council_districts.write \
        .format("jdbc") \
        .option("driver", DB_CONFIG["driver"]) \
        .option("url", DB_CONFIG["url"]) \
        .option("dbtable", LOOKUP_TABLES["city_council_districts"]) \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .mode("overwrite") \
        .save()
    print("✅ City Council Districts Loaded!")
except Exception as e:
    print(f"❌ Error loading City Council Districts: {e}")




read_query1 = "(SELECT distinct ward, precinct FROM boston_311_stage) AS subquery"
# fire_district, pwd_district, city_council_district, police_district, neighborhood_services_district, ward, precinct,

df_electoral_div = spark.read \
    .format("jdbc") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("url", url) \
    .option("dbtable", read_query1) \
    .option("user", user) \
    .option("password", password) \
    .load()

df_electoral_div = df_electoral_div.withColumn(
    "ward", 
    regexp_extract(col("ward"), r"(\d+)", 1).cast("int")  # Extract numeric part from the 'ward' column
)

# Add metadata columns for tracking
df_electoral_div = df_electoral_div.withColumn("db_created_datetime", current_timestamp()) \
                                   .withColumn("db_modified_datetime", lit(None).cast("timestamp")) \
                                   .withColumn("created_by", lit("system")) \
                                   .withColumn("modified_by", lit(None).cast("string")) \
                                   .withColumn("process_id", lit(process_id))


try:
    df_electoral_div.write \
        .format("jdbc") \
        .option("driver", DB_CONFIG["driver"]) \
        .option("url", DB_CONFIG["url"]) \
        .option("dbtable", LOOKUP_TABLES["electoral_div"]) \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .mode("overwrite") \
        .save()
    print("✅ Electoral Divisions Loaded!")
except Exception as e:
    print(f"❌ Error loading Electoral Divisions: {e}")


# Stop the Spark session
spark.stop()
print("✅ Spark session closed.")
