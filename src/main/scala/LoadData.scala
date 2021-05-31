import org.apache.spark.sql.SparkSession
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import java.net.{URI, URLEncoder}
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator, LocatedFileStatus}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SaveMode


// adjustments for cde
// .config("spark.executor.cores", "4")
// .config("spark.num.executors", "2")
// .config("spark.executor.memory", "4G")
// .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
//       "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")


object LoadData {

    def main(args: Array[String]) {

        // starting a SparkSession
        val spark = SparkSession
            .builder()
            .appName("LoadData")
            .enableHiveSupport()
            .getOrCreate()

        val green_trip_data_pre2015 = "s3a://nyc-tlc/trip data/green_tripdata_201[3-4]*.csv"
        val green_pre2015 = spark.read.option("header", true).csv(green_trip_data_pre2015)

        val green_trip_data_2015_h1 = "s3a://nyc-tlc/trip data/green_tripdata_2015-0[1-6].csv"
        val green_2015_h1 = spark.read.option("header", true).csv(green_trip_data_2015_h1)

        val green_trip_data_2015_h2 = "s3a://nyc-tlc/trip data/green_tripdata_2015-{07,08,09,10,11,12}.csv"
        val green_2015_h2 = spark.read.option("header", true).csv(green_trip_data_2015_h2)

        val green_taxi_pre2015 = green_pre2015.withColumnRenamed("Trip_type ", "trip_type")
        val green_taxi_2015_h1 = green_2015_h1.withColumnRenamed("Trip_type ", "trip_type")
        val green_taxi_2015_h2 = green_2015_h2.withColumnRenamed("Trip_type ", "trip_type")

        val data_list = List(green_taxi_pre2015, green_taxi_2015_h1, green_taxi_2015_h2)
        val table_names = List("green_taxi_pre2015", "green_taxi_2015_h1", "green_taxi_2015_h2")
        val entity_list = data_list.zip(table_names)

        def verifyResult(frame: org.apache.spark.sql.DataFrame, name: String) {
            val verificationResult = VerificationSuite()
                .onData(frame)
                .addCheck(
                    Check(CheckLevel.Error, "unit testing my data")
                        .isComplete("lpep_pickup_datetime")
                        .isComplete("lpep_dropoff_datetime")
                        .isComplete("fare_amount")
                    )
                .run()

            if (verificationResult.status == CheckStatus.Success) {
                println("The data passed the test, everything is fine!")
                frame.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable(name)
            } else {
                println("We found errors in the data:\n")

                val resultsForAllConstraints = verificationResult.checkResults
                  .flatMap { case (_, checkResult) => checkResult.constraintResults }

                resultsForAllConstraints
                  .filter { _.status != ConstraintStatus.Success }
                  .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
            }
        }

        for ((table, table_name) <- entity_list) {
            // try deequ test
            verifyResult(table, table_name)
        }

    
        // stop the session
        spark.stop()


    }
}