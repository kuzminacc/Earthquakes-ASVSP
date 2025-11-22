import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# Konfiguracija
HIVE_METASTORE_URIS = os.environ.get("HIVE_SITE_CONF_hive_metastore_uris", "thrift://hive-metastore:9083")

conf = SparkConf().setAppName("HiveTableCheck").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

# Kreiranje SparkSession sa Hive podrskom
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

print("SparkSession created with Hive support")

# Prikaz svih tabela u default bazi
print("Tables in default database:")
spark.sql("SHOW TABLES IN default").show()

# Pokušaj da učitaš tvoju tabelu
try:
    df = spark.table("default.preprocessed_eartquakes")
    print(f"First 5 rows of default.preprocessed_eartquakes:")
    df.show(5)
except Exception as e:
    print("❌ Could not read table:", e)
