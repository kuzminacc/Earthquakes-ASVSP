print("Starting training job...")

import os
import shutil
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import when

# === Konfiguracija Spark aplikacije ===
# Preuzimamo iste promenljive okrzenja koje koristi batch.py
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

conf = SparkConf().setAppName("EarthquakeModelTraining").setMaster("spark://spark-master:7077")

# Kreiramo SparkSession sa Hive podrskom
spark = SparkSession.builder.config(conf=conf).getOrCreate()

print("SparkSession created with Hive support")

# ===  Ucitavanje preprocesiranih podataka iz Hive tabele ===
print("Reading preprocessed data from Hive table 'preprocessed_eartquakes'...")

hdfs_path = "hdfs://namenode:9000/user/hive/warehouse/preprocessed_eartquakes"
df = spark.read.parquet(hdfs_path)


print(f"Number of rows in dataset: {df.count()}")

# ===  Priprema podataka za treniranje modela ===
# Ovde biramo relevantne numericke kolone koje mogu uticati na jacinu zemljotresa.
# Napomena: koristi se iskljucivo float/double kolone jer Spark ML ne podrzava stringove direktno.
feature_cols = ["source_depth_km", "source_gap_deg","source_latitude", "source_longitude"]

label_col = "source_magnitude"

# Zadržavamo samo potrebne kolone
df = df.select(*(feature_cols + [label_col]))

#proveriti ovo
for col_name in feature_cols + [label_col]:
    df = df.withColumn(col_name, when(col(col_name).rlike("^\d+(\.\d+)?$"), col(col_name).cast("float")).otherwise(None))
df = df.dropna()

print("✅ Data prepared for training")

# Train/test split
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"Train rows: {train_df.count()}, Test rows: {test_df.count()}")

# ===  Definisanje modela ===
# Prvo kreiramo vektore karakteristika (features)
# VectorAssembler
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")

# Regressor
rf = RandomForestRegressor(featuresCol="features", labelCol=label_col, numTrees=100, maxDepth=10)

pipeline = Pipeline(stages=[assembler, rf])

# === Treniranje modela ===
print("Training the model...")
model = pipeline.fit(train_df)
print(" Model trained successfully")

# Evaluate
predictions = model.transform(test_df).select("prediction", label_col)
evaluator_rmse = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse")
evaluator_mae = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="mae")
evaluator_r2 = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="r2")

rmse = evaluator_rmse.evaluate(predictions)
mae = evaluator_mae.evaluate(predictions)
r2 = evaluator_r2.evaluate(predictions)

print(f"Evaluation on test set: RMSE={rmse:.4f}, MAE={mae:.4f}, R2={r2:.4f}")


# === FEATURE IMPORTANCE (Random Forest) ===
print("📊 Calculating feature importance...")

# RandomForest model je poslednja faza pipeline-a
rf_model = model.stages[-1]

# Feature importance vector
importances = rf_model.featureImportances.toArray()

# Mapiranje importance -> imena feature-a
feature_importance = list(zip(feature_cols, importances))

# Sortiranje (najuticajniji prvi)
feature_importance.sort(key=lambda x: x[1], reverse=True)

print("\n📊 Feature importance (descending):")
for feature, importance in feature_importance:
    print(f"{feature:25s} -> {importance:.4f}")


# === Cuvanje modela u HDFS (ili lokalno u /models ako imas volumen) ===
# Ovde se koristi shared folder /models koji je mount-ovan u docker-compose fajlu
model_path = "/models/seismic_model"

# Ako folder vec postoji, Spark baca gresku, pa prvo brisemo postojeci model (ako postoji)
import shutil
if os.path.exists(model_path):
    shutil.rmtree(model_path)

model.write().overwrite().save(model_path)
print(f" Model saved successfully at: {model_path}")

print(" Training job completed successfully!")