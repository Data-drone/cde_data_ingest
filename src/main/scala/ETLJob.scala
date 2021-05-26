import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{to_timestamp, col}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

// adjustments for cde
// .config("spark.executor.cores", "4")
// .config("spark.num.executors", "2")
// .config("spark.executor.memory", "4G")
// .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
//       "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")


object ETLJob {

    def main(args: Array[String]) {

        val spark = SparkSession
            .builder()
            .appName("ETL_Job")
            .enableHiveSupport()
            .getOrCreate()

        val green_taxi_pre_2015 = spark.sql("""select vendorid,
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

        val green_taxi_2015_h1 = spark.sql("""select vendorid,
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

        val taxi_test = green_taxi_2015_h1.unionByName(green_taxi_pre_2015, allowMissingColumns=true)

        val formatted = taxi_test
            .withColumn("pickupDateTime", to_timestamp(col("lpep_pickup_datetime"), "yyyy-MM-dd HH:mm:SS"))
            .withColumn("dropOffDateTime", to_timestamp(col("lpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:SS"))
            .withColumn("rate_cod_id", col("ratecodeid").cast(IntegerType))
            .withColumn("passenger_count", col("ratecodeid").cast(IntegerType))
            .withColumn("tripDistance", col("trip_distance").cast(DoubleType))
            .withColumn("fare_amount", col("fare_amount").cast(DoubleType))
            .withColumn("extra", col("extra").cast(DoubleType))
            .withColumn("mta_tax", col("mta_tax").cast(DoubleType))
            .withColumn("tip_amount", col("tip_amount").cast(DoubleType))
            .withColumn("tolls_amount", col("tolls_amount").cast(DoubleType))
            .withColumn("ehail_fee", col("ehail_fee").cast(DoubleType))
            .withColumn("improvement_surcharge", col("improvement_surcharge").cast(DoubleType))
            .withColumn("total_amount", col("total_amount").cast(DoubleType))
            .withColumn("trip_type", col("trip_type").cast(IntegerType))
            .drop("lpep_pickup_datetime")
            .drop("lpep_dropoff_datetime")
            .drop("trip_distance")

        // lets create some area summaries maybe?
        formatted.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable("merged")
        
        spark.stop()
    }

}