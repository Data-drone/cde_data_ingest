from pyspark.sql import SparkSession
import pyspark.sql.functions as F

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

# green data has a few different years
green_trip_pre2015 = "s3a://nyc-tlc/trip data/green_tripdata_201[3-4]*.csv"
green_pre2015 = spark.read.option("header", True).csv(green_trip_pre2015)

green_trip_data_2015_h1 = "s3a://nyc-tlc/trip data/green_tripdata_2015-0[1-6].csv"
green_2015_h1 = spark.read.option("header", True).csv(green_trip_data_2015_h1)

green_trip_data_2015_h2 = "s3a://nyc-tlc/trip data/green_tripdata_2015-{07,08,09,10,11,12}.csv"
green_2015_h2 = spark.read.option("header", True).csv(green_trip_data_2015_h2)


# fix space in "Trip_type "
green_pre2015 = green_pre2015.withColumnRenamed("Trip_type " ,"trip_type")

green_2015_h1 = green_2015_h1.withColumnRenamed("Trip_type " ,"trip_type")

green_2015_h2 = green_2015_h2.withColumnRenamed("Trip_type " ,"trip_type")


green_pre2015.write.format("parquet").mode("overwrite").saveAsTable("green_taxi_pre2015")

green_2015_h1.write.format("parquet").mode("overwrite").saveAsTable("green_taxi_2015_h1")

green_2015_h2.write.format("parquet").mode("overwrite").saveAsTable("green_taxi_2015_h2")

#taxi_test.write.format("parquet").mode("overwrite").saveAsTable("taxi_green")

spark.stop()