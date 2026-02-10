from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, ArrayType
from pyspark.sql.functions import col, from_json, window, avg, expr, count, from_unixtime, explode, split, lit, when, to_timestamp, lag, radians, sin, cos, sqrt, atan2, abs 
from pyspark.sql import functions as F
import os
from pyspark.sql.window import Window
from pyspark.ml import PipelineModel
import smtplib
from email.mime.text import MIMEText
from pyspark.ml.linalg import VectorUDT
from pyspark.sql.functions import udf

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def write_to_mongodb(df, epoch_id, mongo_collection):
    """
    Funkcija za upis podataka u MongoDB sa dinamičkim imenom kolekcije.
    :param df: Spark DataFrame koji želimo da upišemo u MongoDB
    :param epoch_id: ID epohe, koristi se za Spark Streaming
    :param mongo_collection: Dinamičko ime kolekcije u MongoDB-u 
    """
    mongo_uri = "mongodb://mongodb:27017"  # Fiksni URI za povezivanje sa MongoDB
    mongo_db = "earthquake_db"  # Fiksni naziv baze podataka
    
    try:
        df.write.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.output.uri", f"{mongo_uri}/{mongo_db}.{mongo_collection}").mode("append").save()
        print(f"[Epoch {epoch_id}] Podaci upisani u MongoDB ({df.count()} redova).")
    except Exception as e:
        print(f"[Epoch {epoch_id}] Greška prilikom upisa u MongoDB: {e}")

# ---------- Funkcija za slanje mejla ----------
def send_email_alert(row):
    if row.alert:
        subject = f"Earthquake Alert! Predicted Magnitude {row.prediction:.1f}"
        body = f"""
        Predicted earthquake magnitude: {row.prediction:.1f}
        Real magnitude: {row.magnitude}
        Location: {row.place or 'Unknown'}
        Coordinates: ({row.source_latitude}, {row.source_longitude})
        Depth: {row.source_depth_km} km
        Time: {row.time}
        """
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_RECEIVER

        try:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(EMAIL_SENDER, EMAIL_PASSWORD)
                server.send_message(msg)
            print(f"[Alert] Email poslat za earthquake id={row.id}, predikcija={row.prediction:.2f}")
        except Exception as e:
            print(f"[Alert] Greška pri slanju mejla za id={row.id}: {e}")

# ---------- Definisanje sheme podataka ----------

earthquakes = StructType([
    StructField("id", StringType(), True),
    StructField("magnitude", DoubleType(), True),
    StructField("place", StringType(), True),
    StructField("time", DoubleType(), True),
    StructField("gap", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("depth", DoubleType(), True)
])


HDFS_NAMENODE = "hdfs://namenode:9000"
TOPIC = "earthquakes-topic"
KAFKA_BROKER = "kafka1:19092"
ALERT_THRESHOLD = 4  # magnituda iznad koje šaljemo mejl

EMAIL_SENDER = "earthquakealertsasvsp@gmail.com"
EMAIL_PASSWORD = os.environ["EMAIL_PASSWORD"]
EMAIL_RECEIVER = "kuzminacn@gmail.com"
MODEL_PATH = "./models/seismic_model"

spark = SparkSession.builder.appName("StreamingProcessing").getOrCreate()
quiet_logs(spark)
print("Spark session pokrenut.")
# ---------- Učitavanje pipeline modela ----------
model = PipelineModel.load(MODEL_PATH)

print("Model učitan")

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BROKER).option("subscribe", TOPIC).load()

df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), earthquakes).alias("data")).select("data.*")

df = df.withColumn("time", from_unixtime(col("time").cast("long") / 1000))

df = df.withColumnRenamed("depth", "source_depth_km").withColumnRenamed("gap", "source_gap_deg").withColumnRenamed("latitude", "source_latitude").withColumnRenamed("longitude", "source_longitude")

print("Kolone preimenovane za model.")

# ---------- Predikcija ----------
predictions_df = model.transform(df)

# Dodavanje kolone za alert
predictions_df = predictions_df.withColumn(
    "alert",
    when(col("prediction") >= ALERT_THRESHOLD, lit(True)).otherwise(lit(False))
)
# Dodavanje kolona za greske
predictions_df = predictions_df.withColumn(
    "error_abs", abs(col("prediction") - col("magnitude"))
).withColumn(
    "error_squared", (col("prediction") - col("magnitude"))**2
)
# ---------- Upis u MongoDB i slanje mejla ----------
def process_batch(batch_df, epoch_id):

    print(f"[Epoch {epoch_id}] Primljeno {batch_df.count()} novih poruka.")

    if batch_df.rdd.isEmpty():
        return
    
    # Ispis predikcija i greške
    rows = batch_df.select("id", "magnitude", "prediction", "error_abs", "alert", "source_latitude", "source_longitude", "source_depth_km", "source_gap_deg", "place","time").collect()
    for row in rows:
        print(f"ID: {row.id}, Actual: {row.magnitude}, Prediction: {row.prediction:.2f}, "
              f"AbsError: {row.error_abs:.2f}, Alert: {row.alert}, "
              f"Lat: {row.source_latitude}, Lon: {row.source_longitude}, Depth: {row.source_depth_km} , Gap: {row.source_gap_deg}, Place: {row.place}, Time: {row.time}")

    # Upis u MongoDB - samo kolone koje MongoDB može da prihvati
    mongo_df = batch_df.select("id", "magnitude", "prediction", "error_abs", "alert", "source_latitude", "source_longitude", "source_depth_km", "source_gap_deg", "place", "time")

    # Upis u MongoDB
    write_to_mongodb(mongo_df, epoch_id, "realtimePredictions")
    
    # Slanje mejlova za visoku magnitudu
    alert_rows = batch_df.filter(col("alert") == True).collect()
    for row in alert_rows:
        send_email_alert(row)

query = predictions_df.writeStream.trigger(processingTime="30 seconds").outputMode("append").foreachBatch(process_batch).start()
print("[Info] Streaming pokrenut...")
query.awaitTermination()