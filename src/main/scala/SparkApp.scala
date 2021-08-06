import org.apache.spark.sql.SparkSession

object SparkApp extends App {

  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Azure Storage Reader")
    .getOrCreate()

  val dataset = sparkSession.read.format("csv").csv("")
  dataset.show

  sparkSession.stop()
}
