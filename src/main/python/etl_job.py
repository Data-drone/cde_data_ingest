from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pysppark.sql.types import DoubleType

# set these from CDE ui not in spark config as per normal
#.config("spark.executor.cores", "4") \
#.config("spark.num.executors", "2") \
#.config("spark.executor.memory", "4G") \

# Notes on CDE Config
# need to add:
# spark.kerberos.access.hadoopFileSystems s3a://nyc-tlc,s3a://<your_cdp_datalake_bucket>


spark = SparkSession \
    .builder \
    .appName("Load Data") \
    .enableHiveSupport() \
    .getOrCreate()

taxi_test = spark.sql("select * from taxi_green")

formatted = taxi_test \
    .withColumn("pickupDate", F.to_timestamp(F.col("lpep_pickup_datetime"), "yyyy-MM-dd HH:mm:SS")) \
    .withColumn("dropOffDate", F.to_timestamp(F.col("lpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:SS")) \
    .withColumn("tripDistance", F.col("trip_distance").cast(DoubleType())) \
    .drop("lpep_pickup_datetime") \
    .drop("lpep_dropoff_datetime")

formatted.createTempView("formatted_taxi_data")

analysis_df = spark.sql("""SELECT PULocationID, DOLocationID, avg(tripDistance), avg(fare_amount) 
    FROM formatted_taxi_data 
    GROUP BY PULocationID, DOLocationID""")

analysis_df.write.format("parquet").mode("overwrite").saveAsTable("analysed_taxi_data")

spark.stop()