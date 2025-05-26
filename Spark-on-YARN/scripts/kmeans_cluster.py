from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

# Khởi tạo SparkSession
spark = (SparkSession.builder
         .appName("Gold–KMeansClustering")
         .getOrCreate())

# Đọc data đã clean từ Silver và aggregate theo PULocationID
cluster_features = [
    "pickup_hour",
    "trip_miles",
    "trip_duration_min",
    "total_fare",
    "fare_per_mile"
]

raw = spark.read.format("delta") \
    .load("s3a://deltalake/silver/fhvhv_main")

cluster_data = raw.groupBy("PULocationID") \
    .agg(*[avg(f).alias(f) for f in cluster_features])

# Chuẩn bị pipeline: assemble → scale → KMeans
assembler = VectorAssembler(
    inputCols=cluster_features,
    outputCol="raw_features"
)
scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withMean=True,
    withStd=True
)

# Khởi tạo KMeans (mặc định predictionCol="prediction")
k_opt = 4
kmeans = KMeans(
    featuresCol="features",
    k=k_opt,
    seed=42
)

pipeline = Pipeline(stages=[assembler, scaler, kmeans])

# Fit model
model = pipeline.fit(cluster_data)

# Transform và đổi tên prediction → cluster_label
result = model.transform(cluster_data) \
    .select(
        col("PULocationID"),
        col("prediction").alias("cluster_label")
    )

# Lưu kết quả ra Delta Gold layer
result.write \
      .format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .save("s3a://deltalake/gold/kmeans_clusters")

# Dừng Spark session
spark.stop()
