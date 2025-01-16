from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql.functions import col, from_json, window, avg, expr, count, from_unixtime, explode, split, lit, when, to_timestamp, lag, radians, sin, cos, sqrt, atan2
from pyspark.sql import functions as F
import os
from pyspark.sql.window import Window

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

from pyspark.sql import SparkSession

def write_to_mongodb(df, epoch_id, mongo_collection):
    """
    Funkcija za upis podataka u MongoDB sa dinamičkim imenom kolekcije.
    :param df: Spark DataFrame koji želimo da upišemo u MongoDB
    :param epoch_id: ID epohe, koristi se za Spark Streaming
    :param mongo_collection: Dinamičko ime kolekcije u MongoDB-u 
    """
    mongo_uri = "mongodb://mongodb:27017"  # Fiksni URI za povezivanje sa MongoDB
    mongo_db = "earthquake_db"  # Fiksni naziv baze podataka
    
    # Podesite MongoDB output URI i kolekciju
    df.write.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.output.uri", f"{mongo_uri}/{mongo_db}.{mongo_collection}").mode("append").save()



earthquakes = StructType([
    StructField("id", StringType(), True),  # added 'id' (assuming it will come from the data)
    StructField("magnitude", DoubleType(), True),
    StructField("place", StringType(), True),
    StructField("time", TimestampType(), True),
    StructField("updated", TimestampType(), True),  # added 'updated'
    StructField("tz", IntegerType(), True),  # added 'tz'
    StructField("detail", StringType(), True),  # added 'detail'
    StructField("felt", IntegerType(), True),  # added 'felt'
    StructField("cdi", DoubleType(), True),  # added 'cdi'
    StructField("mmi", DoubleType(), True),  # added 'mmi'
    StructField("alert", StringType(), True),  # added 'alert'
    StructField("status", StringType(), True),
    StructField("tsunami", IntegerType(), True),
    StructField("sig", IntegerType(), True),  # added 'sig'
    StructField("net", StringType(), True),  # added 'net'
    StructField("code", StringType(), True),  # added 'code'
    StructField("ids", StringType(), True),  # added 'ids'
    StructField("sources", StringType(), True),  # added 'sources'
    StructField("types", StringType(), True),  # added 'types'
    StructField("nst", IntegerType(), True),  # added 'nst'
    StructField("dmin", DoubleType(), True),  # added 'dmin'
    StructField("rms", DoubleType(), True),  # added 'rms'
    StructField("gap", DoubleType(), True),  # added 'gap'
    StructField("magType", StringType(), True),  # added 'magType'
    StructField("type", StringType(), True),  # added 'type'
    StructField("title", StringType(), True),  # added 'title'
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("depth", DoubleType(), True)
])


HDFS_NAMENODE = "hdfs://namenode:9000"
TOPIC = "earthquakes-topic"
KAFKA_BROKER = "kafka1:19092"


spark = SparkSession.builder.appName("StreamingProcessing").getOrCreate()
quiet_logs(spark)

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BROKER).option("subscribe", TOPIC).load()

df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), earthquakes).alias("data")).select("data.*")

df = df.withColumn("time", from_unixtime(col("time").cast("long") / 1000))

#num_per_hour_df = df.filter(col("magnitude") > 3.0).groupBy(window(col("time"), "60 minutes")).agg(count("*").alias("num_earthquakes_mag_gt3")).select("window.start", "window.end", "num_earthquakes_mag_gt3")

#postgresql_stream=num_per_hour_df.writeStream.trigger(processingTime='120 seconds').outputMode('update').foreachBatch(lambda batch_df, epoch_id: write_to_mongodb(batch_df, epoch_id, "testCollection")).start()

#postgresql_stream.awaitTermination()

#****UPIT 1**** Proscena magnituda zemljotresa u periodima od X minuta
avg_magnitude_df = df.groupBy(window(col("time"), "30 minutes")).agg(avg("magnitude").alias("avg_magnitude"))
mongo_query1_stream = avg_magnitude_df.writeStream.trigger(processingTime='60 seconds').outputMode('update').foreachBatch(lambda batch_df, epoch_id: write_to_mongodb(batch_df, epoch_id, "avgMagnitude")).start()


#*****UPIT 2***** Ukupna energija oslobodjena zemljotresima u poslednjih X minuta
energy_df = df.groupBy(window(col("time"), "30 minutes")).agg(
    F.sum(F.pow(10, 1.5 * col("magnitude") + 4.8)).alias("total_energy")
)
mongo_query2_stream = energy_df.writeStream.trigger(processingTime='60 seconds').outputMode('update').foreachBatch(lambda batch_df, epoch_id: write_to_mongodb(batch_df, epoch_id, "totalEnergy")).start()

''' ***********LOS UPIT***********
#*****UPIT 3***** Pratiti trend broja zemljotresa u poslednjih X minuta (rast, opadanje ili stabilnost)  kraju svakog od petominutnih prozora
count_per_window = df.groupBy(window(col("time"), "30 minutes")).agg(
    count("*").alias("num_earthquakes")
)

# Step 2: Calculate the trend based on the number of earthquakes in each window
trend_df = count_per_window.withColumn(
    "trend",
    when(col("num_earthquakes") > 10, "increase")  # Example: define trend based on some threshold
    .when(col("num_earthquakes") < 5, "decrease")  # Example: define trend based on some threshold
    .otherwise("stable")
)

mongo_query3_stream = trend_df.writeStream.trigger(processingTime='60 seconds').outputMode('update').foreachBatch(lambda batch_df, epoch_id: write_to_mongodb(batch_df, epoch_id, "numberOfEarthquakesTrend")).start()
'''

#****UPIT 4**** Pratiti procenat zemljotresa sa dubinom manjom od 10 km u odnosu na ukupne zemljotrese u poslednjih X minuta.
shallow_quake_percentage_df = df.groupBy(window(col("time"), "30 minutes")).agg(
    (F.sum(when(col("depth") < 10, 1).otherwise(0)) / count("*") * 100).alias("percent_shallow")
)

mongo_query4_stream = shallow_quake_percentage_df.writeStream.trigger(processingTime='60 seconds').outputMode('update').foreachBatch(lambda batch_df, epoch_id: write_to_mongodb(batch_df, epoch_id, "shallowEarthquakesPercentage")).start()


#****UPIT 5**** Prikazati najčešći broj seismičkih stanica koje su detektovale zemljotres u poslednjih X minuta.
average_nst_df = df.groupBy(window(col("time"), "30 minutes")).agg(avg("nst").alias("average_nst")).select("window", "average_nst")

mongo_query5_stream = average_nst_df.writeStream.trigger(processingTime='60 seconds').outputMode('update').foreachBatch(lambda batch_df, epoch_id: write_to_mongodb(batch_df, epoch_id, "averageNst")).start()


#*****UPIT DODATNI**** Pratiti prosečnu udaljenost između epicentara zemljotresa u poslednjih 5 minuta.
#Prikazati broj zemljotresa koji su se dogodili u određenom geografskom regionu (npr. unutar 10 km od određene tačke) u poslednjih 5 minuta.
# Tacka uporedjivanja je Novi Sad, 300KM RADIJUS
target_lat = 34.052235
target_lon = -118.243683
radius = 300
earth_radius_km = 6371  # Poluprečnik Zemlje u km


# Izračunavanje udaljenosti koristeći haversine formulu
region_quake_count_df = df.withColumn(
    "distance", 
    earth_radius_km * 2 * atan2(
        sqrt(
            sin(radians((col("latitude") - lit(target_lat)) / 2)) ** 2 +
            cos(radians(lit(target_lat))) * cos(radians(col("latitude"))) *
            sin(radians((col("longitude") - lit(target_lon)) / 2)) ** 2
        ),
        sqrt(1 - (
            sin(radians((col("latitude") - lit(target_lat)) / 2)) ** 2 +
            cos(radians(lit(target_lat))) * cos(radians(col("latitude"))) *
            sin(radians((col("longitude") - lit(target_lon)) / 2)) ** 2
        ))
    )
)
# Filtriranje samo onih događaja koji su unutar 300 km
region_quake_count_df = region_quake_count_df.filter(col("distance") <= radius)

# Grupisanje po vremenskom prozoru (prilagodite vremenski prozor ako je potrebno)
region_quake_count_df = region_quake_count_df.groupBy(F.window(col("time"), "120 minutes")).count()


mongo_query6_stream = region_quake_count_df.writeStream.trigger(processingTime='240 seconds').outputMode('update').foreachBatch(lambda batch_df, epoch_id: write_to_mongodb(batch_df, epoch_id, "numberOfEarthquakesNearLA")).start()


query = region_quake_count_df.writeStream.outputMode("complete").option("truncate", "false").format("console").start()
query.awaitTermination()



##TREBA DODATI NEKI SA SPAJANJEM JOIN