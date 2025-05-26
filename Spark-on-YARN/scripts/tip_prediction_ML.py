from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

# 1. Khởi tạo SparkSession
spark = (SparkSession.builder
         .appName("Gold–TipPrediction–LR")
         .getOrCreate())

# 2. Đọc dữ liệu đã clean từ Silver
df = (spark.read
      .format("delta")
      .load("s3a://deltalake/silver/fhvhv_main")
      .select(
          "trip_miles",
          "trip_duration_min",
          "total_fare",
          "pickup_hour",
          "pickup_weekday",
          "PULocationID",
          "DOLocationID",
          "shared_match_flag",
          "tips"
      )
)

# 3. Chuyển shared_match_flag Y/N -> numeric
df = df.withColumn(
    "shared_match_flag_num",
    when(col("shared_match_flag") == "Y", 1).otherwise(0)
)

# 4. Assemble features
assembler = VectorAssembler(
    inputCols=[
        "trip_miles",
        "trip_duration_min",
        "total_fare",
        "pickup_hour",
        "pickup_weekday",
        "PULocationID",
        "DOLocationID",
        "shared_match_flag_num"
    ],
    outputCol="features"
)

# 5. Khởi tạo Linear Regression
lr = LinearRegression(
    featuresCol="features",
    labelCol="tips",
    predictionCol="prediction",
    maxIter=100,
    regParam=0.1,
    elasticNetParam=0.0  # 0 = Ridge, 1 = Lasso, 0.0 = pure Ridge
)

# 6. Tạo pipeline và split train/test
pipeline = Pipeline(stages=[assembler, lr])
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# 7. Fit model
model = pipeline.fit(train_df)

# 8. Dự đoán và đánh giá
pred_df = model.transform(test_df)

evaluator_rmse = RegressionEvaluator(
    labelCol="tips",
    predictionCol="prediction",
    metricName="rmse"
)
evaluator_r2 = RegressionEvaluator(
    labelCol="tips",
    predictionCol="prediction",
    metricName="r2"
)

rmse = evaluator_rmse.evaluate(pred_df)
r2   = evaluator_r2.evaluate(pred_df)
print(f"Linear Regression Test RMSE = {rmse:.4f}")
print(f"Linear Regression Test R2   = {r2:.4f}")

# 9. Lưu model & kết quả
model.write().overwrite().save("s3a://deltalake/models/pipelines/tip_lr")

(pred_df
   .select(
       col("prediction").alias("predicted_tips"),
       col("tips").alias("actual_tips"),
       "trip_miles","pickup_hour","PULocationID"
   )
   .write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .save("s3a://deltalake/gold/tip_predictions_lr")
)

spark.stop()
