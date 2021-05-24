from pyspark.sql import SparkSession


# set these from CDE ui not in spark config as per normal
#.config("spark.executor.cores", "4") \
#.config("spark.num.executors", "2") \
#.config("spark.executor.memory", "4G") \

# Notes on CDE Config
# need to add:
# spark.kerberos.access.hadoopFileSystems s3a://nyc-tcl,s3a://<your_cdp_datalake_bucket>


spark = SparkSession \
    .builder \
    .appName("Load Data") \
    .enableHiveSupport() \
    .getOrCreate()

green_trip_data = "s3a://nyc-tlc/trip data/green_tripdata_2018*.csv"
taxi_test = spark.read.option("header", True).csv(green_trip_data)

taxi_test.write.format("parquet").mode("overwrite").saveAsTable("taxi_green")

spark.stop()