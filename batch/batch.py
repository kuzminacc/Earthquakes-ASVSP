import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, LongType, TimestampType
from itertools import combinations
from functools import reduce


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


print("Spark job started")

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

conf = SparkConf().setAppName("Earthquakes").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

spark = SparkSession.builder.config(conf=conf) \
                            .enableHiveSupport() \
                            .getOrCreate()

print("Trying to read from file..." + HDFS_NAMENODE)
quiet_logs(spark)

customSchema = StructType([
    StructField("network_code", StringType(), True),
    StructField("receiver_code", StringType(), True),
    StructField("receiver_type", StringType(), True),
    StructField("receiver_latitude", FloatType(), True),
    StructField("receiver_longitude", FloatType(), True),
    StructField("receiver_elevation_m", FloatType(), True),
    StructField("p_arrival_sample", FloatType(), True),  # Nullable
    StructField("p_status", StringType(), True),  # Nullable
    StructField("p_weight", FloatType(), True),  # Nullable
    StructField("p_travel_sec", DoubleType(), True),  # Nullable
    StructField("s_arrival_sample", FloatType(), True),  # Nullable
    StructField("s_status", StringType(), True),  # Nullable
    StructField("s_weight", FloatType(), True),  # Nullable
    StructField("source_id", StringType(), True),
    StructField("source_origin_time", TimestampType(), True),
    StructField("source_origin_uncertainty_sec", StringType(), True),  # Nullable
    StructField("source_latitude", FloatType(), True),
    StructField("source_longitude", FloatType(), True),
    StructField("source_error_sec", StringType(), True),  # Nullable
    StructField("source_gap_deg", StringType(), True),  # Nullable
    StructField("source_horizontal_uncertainty_km", StringType(), True),  # Nullable
    StructField("source_depth_km", StringType(), True),  # Nullable
    StructField("source_depth_uncertainty_km", StringType(), True),  # Nullable
    StructField("source_magnitude", FloatType(), True),  # Nullable
    StructField("source_magnitude_type", StringType(), True),  # Nullable
    StructField("source_magnitude_author", StringType(), True),  # Nullable
    StructField("source_mechanism_strike_dip_rake", StringType(), True),  # Nullable
    StructField("source_distance_deg", FloatType(), True),  # Nullable
    StructField("source_distance_km", FloatType(), True),  # Nullable
    StructField("back_azimuth_deg", FloatType(), True),  # Nullable
    StructField("snr_db", FloatType(), True),  # Nullable
    StructField("coda_end_sample", StringType(), True),  # Nullable
    StructField("trace_start_time", TimestampType(), True),
    StructField("trace_category", StringType(), True),
    StructField("trace_name", StringType(), True)
])


df = spark.read.format("csv").option("header","true").schema(customSchema).load(
    HDFS_NAMENODE + "/data/merge.csv")
df.show(5)  # Show the first 5 rows
print("Raw data picked up")