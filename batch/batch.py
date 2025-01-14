import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, LongType, TimestampType
from itertools import combinations
from functools import reduce
from pyspark.sql.functions import to_timestamp, date_trunc, col, when, split, trim, regexp_replace
import math
import datetime

# Funkcija za računanje Haversine udaljenosti u kilometrima
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Poluprečnik Zemlje u kilometrima
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    
    a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c  # Udaljenost u kilometrima

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def write_df(dataframe,tablename):
    PSQL_SERVERNAME= "postgres"
    PSQL_PORTNUMBER = 5432
    PSQL_DBNAME = "postgres"
    PSQL_USERNAME = "postgres"
    PSQL_PASSWORD = "postgres"
    URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"

    dataframe.write.format("jdbc").options(
        url=URL,
        driver="org.postgresql.Driver",
        user=PSQL_USERNAME,
        password=PSQL_PASSWORD,
        dbtable=tablename
    ).mode("overwrite").save()

def preprocess_data(df, table_name):
    
  # Trunciranje vremena na sekunde, OVO MOZDA I UKOLINITI NISAM SIGURNA, VIDECEMO KAKKO BUDE ISLO, MOZDA OSTANU MILISEKUNDE
  df = df.withColumn("source_origin_time", date_trunc("second", "source_origin_time")).withColumn("trace_start_time", date_trunc("second", "trace_start_time"))

  # Proveravamo da li je kolona NULL pre nego što nastavimo sa obradom
  '''
  df = df.withColumn(
      "source_mechanism_strike_dip_rake_clean",
      when(
          col("source_mechanism_strike_dip_rake").isNotNull(),
          regexp_replace(col("source_mechanism_strike_dip_rake"), r"^\[\[|\]\]$", "")  # Uklanjanje [[ i ]]
      ).otherwise(None)  # Postavljamo na NULL ako je originalna kolona NULL
  )

  # Zamenjivanje višestrukih razmaka jednim razmakom
  df = df.withColumn(
      "source_mechanism_strike_dip_rake_clean",
      when(
          col("source_mechanism_strike_dip_rake_clean").isNotNull(),
          regexp_replace(col("source_mechanism_strike_dip_rake_clean"), r"\s+", " ")  # Zamenjujemo višestruke razmake sa jednim
      ).otherwise(None)
  )

  # Razdvajanje 'source_mechanism_strike_dip_rake_clean' na strike, dip i rake
  df = df.withColumn(
      "strike",
      when(
          col("source_mechanism_strike_dip_rake_clean").isNotNull(),
          split(col("source_mechanism_strike_dip_rake_clean"), " ")[0].cast(FloatType())
      ).otherwise(None)
  ).withColumn(
      "dip",
      when(
          col("source_mechanism_strike_dip_rake_clean").isNotNull(),
          split(col("source_mechanism_strike_dip_rake_clean"), " ")[1].cast(FloatType())
      ).otherwise(None)
  ).withColumn(
      "rake",
      when(
          col("source_mechanism_strike_dip_rake_clean").isNotNull(),
          split(col("source_mechanism_strike_dip_rake_clean"), " ")[2].cast(FloatType())
      ).otherwise(None)
  )
    
  # Uklanjanje originalne kolone "source_mechanism_strike_dip_rake"
  df = df.drop("source_mechanism_strike_dip_rake")
'''
  # Uklanjanje "[]", tako da ostane samo broj
  df = df.withColumn(
      "coda_end_sample",  # Ista kolona, samo očisti vrednost
      F.regexp_replace("coda_end_sample", r"\[\[|\]\]", "")
  )

  # Pretvori string u numerički tip (ako je potrebno)
  df = df.withColumn("coda_end_sample", df["coda_end_sample"].cast("float"))


  # 3. Uključivanje transformacije na sve numeričke vrednosti ***********************TESTIRATI DA LI TREBA JOS NESTO DA SE DODA***************************
  numeric_columns = ["receiver_latitude", "receiver_longitude", "receiver_elevation_m", 
                      "p_arrival_sample", "p_weight", "p_travel_sec", "s_arrival_sample", "s_weight", 
                      "source_latitude", "source_longitude", #"source_depth_km"
                      "source_magnitude", "source_distance_deg", "source_distance_km", "back_azimuth_deg", 
                    ]

    # Primena transformacija na sve numeričke kolone
  for col_name in numeric_columns:
    df = df.withColumn(
        col_name,
        F.when(F.col(col_name).isNotNull(),
              F.regexp_replace(F.col(col_name).cast("string"), r"(\d+)\.$", r"\1.0").cast(FloatType()))
        .otherwise(None)
    )

  df.write.mode("overwrite").saveAsTable(table_name)



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
    StructField("source_origin_time", TimestampType(), True), #PROVERITI KOJI TIP DA BUDE
    StructField("source_origin_uncertainty_sec", StringType(), True),  # Nullable, JER JE ILI NAN ILI FLOAT
    StructField("source_latitude", FloatType(), True),
    StructField("source_longitude", FloatType(), True),
    StructField("source_error_sec", StringType(), True),  # Nullable, JER JE ILI NAN ILI FLOAT
    StructField("source_gap_deg", StringType(), True),  # Nullable, JER JE ILI NAN ILI FLOAT
    StructField("source_horizontal_uncertainty_km", StringType(), True),  # Nullable, JER JE ILI NONE ILI FLOAT
    StructField("source_depth_km", StringType(), True),  # Nullable, MOZDA DA BUDE FLOAT
    StructField("source_depth_uncertainty_km", StringType(), True),  # Nullable, JER JE ILI NONE ILI FLOAT
    StructField("source_magnitude", FloatType(), True),  # Nullable
    StructField("source_magnitude_type", StringType(), True),  # Nullable
    StructField("source_magnitude_author", StringType(), True),  # Nullable
    StructField("source_mechanism_strike_dip_rake", StringType(), True),  # Nullable ovaj format [[ 67.0 34.6 56.7 ]]
    StructField("source_distance_deg", FloatType(), True),  # Nullable
    StructField("source_distance_km", FloatType(), True),  # Nullable
    StructField("back_azimuth_deg", FloatType(), True),  # Nullable
    StructField("snr_db", StringType(), True),  # Nullable ovaj format [67.0 34.6 56.7 ]
    StructField("coda_end_sample", StringType(), True),  # Nullable [[555.6]]
    StructField("trace_start_time", TimestampType(), True),
    StructField("trace_category", StringType(), True),
    StructField("trace_name", StringType(), True)
])


df = spark.read.format("csv").option("header","true").option("multiLine", "true").schema(customSchema).load(
    HDFS_NAMENODE + "/data/merge.csv")
row_count = df.count()
print(f"Broj učitanih redova: {row_count}")

print("Raw data picked up")
preprocess_data(df, "preprocessed_eartquakes")
print("Preprocessing done")

#df1 = spark.table("preprocessed_eartquakes")
#df1.show(1)
#row_count1 = df1.count()
#print(f"Broj učitanih redova nakon pretprocesiranja: {row_count1}")

df = spark.table("preprocessed_eartquakes")

# ### PROBA sa spark sql
#sqlDf = spark.sql("SELECT count(*) FROM preprocessed_eartquakes;")
#sqlDf.show()
#print("Query proba done")



# ********UPIT 1********
# Filtriraj i transformiši podatke
filtered_df = df.filter(
    (F.col("source_origin_time").isNotNull()) &
    (F.col("source_magnitude").isNotNull()) &
    (F.col("source_depth_km").isNotNull()) &
    (F.col("source_origin_time") >= F.date_sub(F.current_date(), 30 * 365))
)

# Dodaj rangiranje
window_spec = Window.partitionBy(F.year("source_origin_time"), "network_code").orderBy(F.col("source_magnitude").desc())
ranked_df = filtered_df.withColumn(
    "year", F.year("source_origin_time")
).withColumn(
    "rank", F.rank().over(window_spec)
).withColumn(
    "month", F.month("source_origin_time")
).withColumn(
    "day", F.dayofmonth("source_origin_time")
)

# Zadrži samo relevantne podatke
curated_df = ranked_df.filter(F.col("rank") == 1).select(
    "year", "month", "day", "network_code", "source_id", "source_magnitude", "source_depth_km"
)
# Sortiraj prema godini i regionu (network_code)
sorted_df = curated_df.orderBy("year", "network_code")
write_df(sorted_df, "BiggestEarthquakesIn30Years")
# Prikazivanje rezultata (ako je potrebno)
# Ispis broja redova u DataFrame-u
#print(f"Broj redova: {sorted_df.count()}")
#sorted_df.show(5)


#******UPIT 2 *******


# Step 1: Filter and group by quarter and region to calculate average magnitude
quarterly_avg_df = df.filter(
    (F.col("source_origin_time") >= F.date_sub(F.current_date(), 10 * 365)) &  # Filter last 5 years
    (F.col("source_magnitude").isNotNull())  # Exclude rows with NULL magnitudes
).withColumn(
    "quarter", F.concat(
        F.year("source_origin_time"), 
        F.lit("-Q"), 
        F.quarter("source_origin_time")
    )
).groupBy(
    "quarter", "network_code"
).agg(
    F.avg("source_magnitude").alias("avg_magnitude")
)
#print("Posle prvog filtera UPIT 2: ", quarterly_avg_df.count())

window_spec = Window.partitionBy("network_code").orderBy("quarter")

# Step 3: Calculate the lag of the average magnitude for previous quarter
quarterly_avg_df_with_lag = quarterly_avg_df.withColumn(
    "prev_quarter_avg", F.lag("avg_magnitude").over(window_spec)
)

#quarterly_avg_df_with_lag.show(3)


# Step 4: Calculate the change from the previous quarter
quarterly_avg_df_final = quarterly_avg_df_with_lag.withColumn(
    "change_from_prev_quarter", 
    F.col("avg_magnitude") - F.col("prev_quarter_avg")
)

#quarterly_avg_df_final.show(3)

# Step 5: Select the final columns and order by region and quarter
final_df = quarterly_avg_df_final.select(
    "quarter",
    "network_code",
    "avg_magnitude",
    "prev_quarter_avg",
    "change_from_prev_quarter"
).orderBy("network_code", "quarter")

write_df(final_df, "AverageMagnitudeTrendIn10Years")


#******UPIT 3*******

# Step 1: Filter the data to remove invalid values (NULL and 'None')
df_filtered = df.filter(
    (F.col("source_gap_deg").isNotNull()) & 
    (F.col("source_gap_deg") != 'None') &
    (F.col("source_gap_deg") != 'nan')
)
#print("Posle prvog filtera: ", df_filtered.count())
# Step 2: Cast source_gap_deg to FloatType for numeric calculations
df_filtered = df_filtered.withColumn(
    "source_gap_deg_float", F.col("source_gap_deg").cast("float")
)
#print("Posle drugog filtera: ",df_filtered.count())

# Step 3: Calculate avg_gap and std_dev_gap after filtering
stats = df_filtered.select(
    F.avg("source_gap_deg_float").alias("avg_gap"),
    F.stddev("source_gap_deg_float").alias("std_dev_gap")
)

# Step 4: Join the stats with the filtered data
df_with_stats = df_filtered.crossJoin(stats)

# Step 5: Add gap_category column using the CASE equivalent (when)
df_with_gap_category = df_with_stats.withColumn(
    "gap_category",
    F.when(
        F.col("source_gap_deg_float") > (F.col("avg_gap") + 2 * F.col("std_dev_gap")),
        "Anomalous"
    ).otherwise("Normal")
)

# Step 6: Order the results
final_df = df_with_gap_category.orderBy(
    F.desc("gap_category"), F.desc("source_magnitude")
)

# Step 7: Select the desired columns
final_df = final_df.select(
    "source_id",
    "source_magnitude",
    "source_gap_deg",
    "source_horizontal_uncertainty_km",
    "source_depth_uncertainty_km",
    "avg_gap",
    "std_dev_gap",
    "gap_category"
)
write_df(final_df, "AzimuthalGapEarthquakes")

#******UPIT 4*******
# Step 1: Kreiranje NTILE kolone (depth_quartile)
window_spec = Window.partitionBy("network_code").orderBy("source_depth_km")

earthquakes_with_quartiles = df.filter(
    F.col("source_depth_km").isNotNull()
).withColumn(
    "depth_quartile", F.ntile(3).over(window_spec)
)

# Step 2: Agregacija po regionu i depth_quartile
aggregated_df = earthquakes_with_quartiles.groupBy(
    "network_code", "depth_quartile"
).agg(
    F.avg("source_magnitude").alias("avg_magnitude"),
    F.min("source_magnitude").alias("min_magnitude"),
    F.max("source_magnitude").alias("max_magnitude")
).orderBy("network_code", "depth_quartile")
#aggregated_df.show(3)
write_df(aggregated_df, "DepthMagnitudeCorrelation")

#********UPIT 5 *********
# Step 1: Filtriranje podataka i dodavanje kolona za razlike u vremenu i godinu
travel_times = df.filter(
    (F.col("p_travel_sec").isNotNull()) & (F.col("source_distance_km").isNotNull())  # Koristimo samo podatke sa p_travel_sec i source_distance_km
).withColumn(
    "s_travel_sec", F.col("source_distance_km") / 3.5  # Aproksimacija vremena putovanja S-talasa (brzina S-talasa = 3.5 km/s)
).withColumn(
    "time_difference", F.col("s_travel_sec") - F.col("p_travel_sec")  # Razlika između vremena putovanja S i P talasa
).withColumn(
    "year", F.year("source_origin_time")  # Ekstraktovanje godine iz vremena porekla
)

# Step 2: Dodavanje prethodne vrednosti razlike vremena unutar iste godine
window_spec_lag = Window.partitionBy("year").orderBy("source_origin_time")
travel_times_with_lag = travel_times.withColumn(
    "prev_time_difference", F.lag("time_difference").over(window_spec_lag)  # Prethodna vrednost razlike vremena
)

# Step 3: Agregacija po godini
aggregated_times = travel_times_with_lag.groupBy("year").agg(
    F.avg("time_difference").alias("avg_time_difference"),  # Prosečna razlika vremena
    F.max("time_difference").alias("max_time_difference"),  # Maksimalna razlika vremena
)

# Step 4: Izračunavanje promene u prosečnoj razlici vremena iz prethodne godine
window_spec_year = Window.orderBy("year")
aggregated_with_change = aggregated_times.withColumn(
    "change_from_prev_year", 
    F.col("avg_time_difference") - F.lag("avg_time_difference").over(window_spec_year)  # Promena u prosečnoj razlici vremena
)

# Step 5: Prikaz rezultata
aggregated_with_change.select(
    "year",
    "avg_time_difference",
    "max_time_difference",
    "change_from_prev_year"
).orderBy("year")
#aggregated_with_change.show(10)
write_df(aggregated_with_change, "TimeDifferenceBetweenPAndS")


#******UPIT 6**********
# Step 1: Filtriranje podataka i dodavanje ponderisane magnitude
weighted_magnitude_df = df.filter(
    (F.col("source_magnitude").isNotNull()) & (F.col("source_distance_km").isNotNull())
).withColumn(
    "weighted_magnitude", F.col("source_magnitude") / (1 + F.col("source_distance_km"))
).withColumn(
    "year", F.year("source_origin_time")  # Ekstraktovanje godine iz vremena porekla
)

# Step 2: Agregacija po godini
aggregated_df = weighted_magnitude_df.groupBy("year").agg(
    F.avg("weighted_magnitude").alias("avg_weighted_magnitude"),
    F.avg("source_magnitude").alias("avg_magnitude")
).orderBy("year")
#aggregated_df.show(5)
write_df(aggregated_df, "AverageMagnitudeWithDistance")

#*********UPTI 7********
# Step 1: Dodavanje kolone za kvintil na osnovu source_distance_km
# Step 1: Replace 'None', 'nan', and null with actual nulls and convert to FloatType
df_cleaned = df.withColumn(
    "source_error_sec", 
    F.when(
        (F.col("source_error_sec") == 'None') | (F.col("source_error_sec") == 'nan') | (F.col("source_error_sec").isNull()), 
        None  # Replace 'None', 'nan', and null with None (which is equivalent to null)
    ).otherwise(F.col("source_error_sec").cast(FloatType()))  # Convert the column to FloatType
)

# Step 2: Filter rows where source_error_sec is not null
df_cleaned = df_cleaned.filter(F.col("source_error_sec").isNotNull())

# Step 3: Add quintile column based on source_distance_km
window_spec = Window.orderBy("source_distance_km")

distance_analysis_df = df_cleaned.withColumn(
    "distance_quintile", F.ntile(10).over(window_spec)  # NTILE(3) for 3 quintiles
)

# Step 4: Aggregate by quintiles
aggregated_df = distance_analysis_df.groupBy("distance_quintile").agg(
    F.avg("source_error_sec").alias("avg_error_sec"),  # Average error in seconds
    F.count("*").alias("event_count")  # Count of events in each quintile
).orderBy("distance_quintile")

# Show results
#aggregated_df.show()

# Write the results
write_df(aggregated_df, "DistanceAndErrorCorrelation")



#******UPIT 8********
# Step 1: Agregacija po regionu (network_code) i brojanje događaja
region_activity_df = df.groupBy("network_code").agg(
    F.count("*").alias("event_count")
)

# Step 2: Ukupno brojanje događaja (total_events) koristeći window funkciju
window_spec = Window.orderBy(F.lit(1))  # Sve redove u okviru jednog okvira (total events)
region_activity_df = region_activity_df.withColumn(
    "total_events", F.sum("event_count").over(window_spec)
)

# Step 3: Izračunavanje procenta za svaki region
region_activity_df = region_activity_df.withColumn(
    "event_percentage", (F.col("event_count") * 1.0 / F.col("total_events")) * 100
)

# Step 4: Sortiranje po broju događaja
sorted_region_activity_df = region_activity_df.orderBy("event_count", ascending=False)
write_df(sorted_region_activity_df,"EventsByRegion")
# Prikazivanje rezultata
#sorted_region_activity_df.show()


#**********UPIT 9********
# Step 1: Računanje vremena obrade (processing_time) i statistike za svaku mrežu (region)
processing_time_df = df.filter(
    (F.col("source_origin_time").isNotNull()) & (F.col("trace_start_time").isNotNull())
).withColumn(
    "processing_time", 
    (F.unix_timestamp("trace_start_time") - F.unix_timestamp("source_origin_time"))
)

# Step 2: Korisćenje window funkcije za izračunavanje prosečnog vremena obrade i standardne devijacije po regionu
window_spec = Window.partitionBy("network_code")

processing_time_df = processing_time_df.withColumn(
    "avg_processing_time", F.avg("processing_time").over(window_spec)
).withColumn(
    "std_dev_processing_time", F.stddev("processing_time").over(window_spec)
)

# Step 3: Klasifikacija obrade kao 'Anomalous' ili 'Normal'
processing_time_df = processing_time_df.withColumn(
    "processing_status",
    F.when(
        F.col("processing_time") > (F.col("avg_processing_time") + 2 * F.col("std_dev_processing_time")),
        "Anomalous"
    ).otherwise("Normal")
)

# Step 4: Selektovanje potrebnih kolona
final_df = processing_time_df.select(
    "network_code", "source_id", "processing_time", 
    "avg_processing_time", "std_dev_processing_time", "processing_status"
)

# Step 5: Sortiranje po regionu i vremenu obrade
final_df = final_df.orderBy("network_code", "processing_time", ascending=False)
write_df(final_df,"TimeProcessingAnomalies")
# Prikazivanje rezultata
#final_df.show(10)







#*******UPIT 10******* FIX ME
# Registruj funkciju kao UDF (User Defined Function) u Spark-u
haversine_udf = F.udf(haversine, returnType=DoubleType())

# Step 1: Dodavanje kolone za udaljenost između stanice i izvora
distance_analysis_df = df.filter(
    F.col("receiver_latitude").isNotNull() &
    F.col("receiver_longitude").isNotNull() &
    F.col("source_latitude").isNotNull() &
    F.col("source_longitude").isNotNull()
).withColumn(
    "distance_km", 
    haversine_udf(
        F.col("receiver_latitude"), 
        F.col("receiver_longitude"), 
        F.col("source_latitude"), 
        F.col("source_longitude")
    )
)

# Step 2: Dodavanje kategorije na osnovu udaljenosti
distance_analysis_df = distance_analysis_df.withColumn(
    "distance_category", 
    F.when(F.col("distance_km") < 50, "Near")
    .when((F.col("distance_km") >= 50) & (F.col("distance_km") <= 100), "Medium")
    .otherwise("Far")
)

# Step 3: Prikazivanje rezultata
distance_analysis_df.select("network_code", "source_id", "distance_km", "source_error_sec", "distance_category") #treba uraditit select pa onda upisati u bazu...ispravi
# Step 3: Selektovanje kolona za upis u bazu
selected_df = distance_analysis_df.select(
    "network_code", 
    "source_id", 
    "distance_km", 
    "source_error_sec", 
    "distance_category"
)
write_df(selected_df,"EarthquakeStationDistance")
print("Trenutno vreme: ", datetime.datetime.now())