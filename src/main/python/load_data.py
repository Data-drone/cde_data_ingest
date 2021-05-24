# Notes on CDE Config
# need to add:
# spark.kerberos.access.hadoopFileSystems s3a://nyc-tcl,s3a://<your_cdp_datalake_bucket>

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Load Data") \
    .enableHiveSupport() \
    .getOrCreate()

green_trip_data = "s3a://nyc-tlc/trip data/green_tripdata_2018*.csv"
taxi_test = spark.read.option("header", True).csv(green_trip_data)

taxi_test.write.format("parquet").option(mode='overwrite').saveAtTable("taxi_green")

spark.stop()