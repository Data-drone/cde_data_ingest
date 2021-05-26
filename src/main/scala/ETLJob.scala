import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{to_timestamp, col}
import org.apache.spark.sql.types._

// adjustments for cde
// .config("spark.executor.cores", "4")
// .config("spark.num.executors", "2")
// .config("spark.executor.memory", "4G")
// .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
//       "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")


object ETLJob {

    def main() {
        val spark = SparkSession
            .builder()
            .appName("LoadData")
            .enableHiveSupport()
            .getOrCreate()

        val taxi_test = spark.sql("""
                                    select * from taxi_green
                                """)

        val formatted = taxi_test
            .withColumn("pickupDate", to_timestamp(col("lpep_pickup_datetime"), "yyyy-MM-dd HH:mm:SS"))
            .withColumn("dropOffDate", to_timestamp(col("lpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:SS"))
            .withColumn("tripDistance", col("trip_distance").cast(DoubleType))
            .drop("lpep_pickup_datetime")
            .drop("lpep_dropoff_datetime")

        // lets create some area summaries maybe?
        
        
    }

}