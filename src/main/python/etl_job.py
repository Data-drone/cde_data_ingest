from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType

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

green_taxi_pre_2015 = spark.sql("""select vendorid,
                lpep_pickup_datetime,
                lpep_dropoff_datetime,
                store_and_fwd_flag,
                ratecodeid,
                pickup_longitude,
                pickup_latitude,
                dropoff_longitude,
                dropoff_latitude,
                passenger_count,
                trip_distance,
                fare_amount,
                extra,
                mta_tax,
                tip_amount,
                tolls_amount,
                ehail_fee,
                total_amount,
                payment_type,
                trip_type
            from green_taxi_pre2015""")

green_taxi_2015_h1 = spark.sql("""select vendorid,
                lpep_pickup_datetime,
                lpep_dropoff_datetime,
                store_and_fwd_flag,
                ratecodeid,
                pickup_longitude,
                pickup_latitude,
                dropoff_longitude,
                dropoff_latitude,
                passenger_count,
                trip_distance,
                fare_amount,
                extra,
                mta_tax,
                tip_amount,
                tolls_amount,
                ehail_fee,
                improvement_surcharge,
                total_amount,
                payment_type,
                trip_type
            from green_taxi_2015_h1""")

taxi_test = green_taxi_2015_h1.unionByName(green_taxi_pre_2015, allowMissingColumns=True)

formatted = taxi_test \
    .withColumn("pickupDateTime", F.to_timestamp(F.col("lpep_pickup_datetime"), "yyyy-MM-dd HH:mm:SS")) \
    .withColumn("dropOffDateTime", F.to_timestamp(F.col("lpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:SS")) \
    .withColumn("rate_cod_id", F.col("ratecodeid").cast(IntegerType())) \
    .withColumn("passenger_count", F.col("ratecodeid").cast(IntegerType())) \
    .withColumn("tripDistance", F.col("trip_distance").cast(DoubleType())) \
    .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType())) \
    .withColumn("extra", F.col("extra").cast(DoubleType())) \
    .withColumn("mta_tax", F.col("mta_tax").cast(DoubleType())) \
    .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType())) \
    .withColumn("tolls_amount", F.col("tolls_amount").cast(DoubleType())) \
    .withColumn("ehail_fee", F.col("ehail_fee").cast(DoubleType())) \
    .withColumn("improvement_surcharge", F.col("improvement_surcharge").cast(DoubleType())) \
    .withColumn("total_amount", F.col("total_amount").cast(DoubleType())) \
    .withColumn("trip_type", F.col("trip_type").cast(IntegerType())) \
    .drop("lpep_pickup_datetime") \
    .drop("lpep_dropoff_datetime") \
    .drop("trip_distance")

formatted.createTempView("formatted_taxi_data")

#analysis_df = spark.sql("""SELECT PULocationID, DOLocationID, avg(tripDistance) as mean_tripDistance, 
#    avg(fare_amount) as mean_fare_amount 
#    FROM formatted_taxi_data 
#    GROUP BY PULocationID, DOLocationID""")

formatted.write.format("parquet").mode("overwrite").saveAsTable("merged")

spark.stop()