import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import schema.HotelSchema
import structure.{Hotel, Weather}
import tools.{HotelRowsCorrector, WeatherRowsCorrector}
import utils.ConfigReader

object SparkApp extends App {

  val azureStorageConfig = ConfigReader.getConfig(System.getenv("AZURE_STORAGE_PROPERTIES"))
  val hotelsSchema = ScalaReflection.schemaFor[HotelSchema].dataType.asInstanceOf[StructType]

  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Weather by Hotels Provider")
    .config(ConfigReader.getAuthConfig(System.getenv("AZURE_ACCOUNT_AUTH_PROPERTIES")))
    .getOrCreate()
  val hotelsDf = sparkSession.read.schema(hotelsSchema).csv(azureStorageConfig("hotels"))
  val weatherDf = sparkSession.read.parquet(azureStorageConfig("weather"))
  val hotelRowsCorrector = new HotelRowsCorrector
  val weatherRowsCorrector = new WeatherRowsCorrector
  try {
    val correctHotelRows = hotelRowsCorrector.addGeohashToCorrectRows(hotelsDf)
    val fixedIncorrectHotelRows = hotelRowsCorrector.fixIncorrectRows(hotelsDf)

    val enrichedHotels = correctHotelRows.union(fixedIncorrectHotelRows)
    val enrichedWeather = weatherRowsCorrector.avgTempByGeoHash(weatherDf)

        val join = enrichedHotels.join(enrichedWeather, enrichedWeather(Weather.geohash) === enrichedHotels(Hotel.geohash), "left").drop(Weather.geohash)

        join.write.partitionBy(Weather.year, Weather.month, Weather.day).parquet(azureStorageConfig("output.path"))
  }
  finally {
    sparkSession.stop()
  }
}

//todo write unit tests
//todo Deploy Spark job on Azure Kubernetes Service (AKS), to setup infrastructure use terraform scripts from module. For this use Running Spark on Kubernetes deployment guide and corresponding to your spark version docker image. Default resource parameters (specifically memory) will not work because of free tier limitations. You needed to setup memory and cores properly.
//todo Store enriched data (joined data with all the fields from both datasets) in provisioned with terraform Azure ADLS gen2 storage preserving data partitioning in parquet format in “data” container (it marked with prevent_destroy=true and will survive terraform destroy).
//todo docker
//todo Readme
//todo  file with link on repo, fully documented homework with screenshots and comments.
//todo DO NOT FORGET TO DELETE UNNECESSARY RESOURCES IN THE CLOUD VIA "TERRAFORM DESTROY", WHEN YOU FINISH WORK WITH THEM!
//todo the result should be by day in year
//todo memory management