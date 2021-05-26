import org.apache.spark.sql.SparkSession
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import java.net.{URI, URLEncoder}
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator, LocatedFileStatus}
import scala.collection.mutable.ListBuffer

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

        val green_trip_data = "s3a://nyc-tlc/trip data/green_tripdata_2018*.csv"
        val taxi_test = spark.read.option("header", true).csv(green_trip_data)

        // try deequ test
        val verificationResult = VerificationSuite()
            .onData(taxi_test)
            .addCheck(
                Check(CheckLevel.Error, "unit testing my data")
                    .isComplete("lpep_pickup_datetime")
                    .isComplete("fare_amount")
                )
            .run()

        if (verificationResult.status == CheckStatus.Success) {
          println("The data passed the test, everything is fine!")
          taxi_test.write.format("parquet").saveAsTable("taxi_green")
        } else {
          println("We found errors in the data:\n")

          val resultsForAllConstraints = verificationResult.checkResults
            .flatMap { case (_, checkResult) => checkResult.constraintResults }

          resultsForAllConstraints
            .filter { _.status != ConstraintStatus.Success }
            .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
        }

        // stop the session
        spark.stop()


    }
}