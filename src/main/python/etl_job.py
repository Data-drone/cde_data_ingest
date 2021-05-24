from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .appName("Load Data") \
    .config("spark.executor.cores", "4") \
    .config("spark.num.executors", "2") \
    .config("spark.executor.memory", "4G") \
    .enableHiveSupport() \
    .getOrCreate()

taxi_test = spark.sql("select * from taxi_green")

formatted = taxi_test \
    .withColumn("pickupDate", F.to_timestamp(F.col("lpep_pickup_datetime"), "yyyy-MM-dd HH:mm:SS")) \
    .withColumn("dropOffDate", F.to_timestamp(F.col("lpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:SS")) \
    .withColumn("tripDistance", F.col("trip_distance").cast("float")) \
    .drop("lpep_pickup_datetime") \
    .drop("lpep_dropoff_datetime")

formatted.createTempView("formatted_taxi_data")

analysis_df = spark.sql("""SELECT PULocationID, DOLocationID, avg(trip_distance), avg(fare_amount) 
    FROM formatted_taxi_data 
    GROUP BY PULocationID, DOLocationID""")

analysis_df.write.format("parquet").option(mode='overwrite').saveAtTable("analysed_taxi_data")

spark.stop()