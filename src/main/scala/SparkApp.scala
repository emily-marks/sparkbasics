import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import schema.HotelSchema
import structure.{Hotel, Weather}
import tools.{HotelRowsCorrector, WeatherRowsCorrector}

object SparkApp extends App {
  val hotelsSchema = ScalaReflection.schemaFor[HotelSchema].dataType.asInstanceOf[StructType]
  val sparkSession = SparkSession.builder().getOrCreate()
  val hotelsDf = sparkSession.read.schema(hotelsSchema).csv("/hotels")
  val weatherDf = sparkSession.read.parquet("/weather")
  val hotelRowsCorrector = new HotelRowsCorrector
  val weatherRowsCorrector = new WeatherRowsCorrector
  try {
    val correctHotelRows = hotelRowsCorrector.addGeohashToCorrectRows(hotelsDf)
    val fixedIncorrectHotelRows = hotelRowsCorrector.fixIncorrectRows(hotelsDf)

    val enrichedHotels = correctHotelRows.union(fixedIncorrectHotelRows)
    val enrichedWeather = weatherRowsCorrector.avgTempByGeoHash(weatherDf)

    val join = enrichedHotels.join(enrichedWeather, enrichedWeather(Weather.geohash) === enrichedHotels(Hotel.geohash), "left").drop(Weather.geohash)
    join.write.partitionBy(Weather.year, Weather.month, Weather.day).parquet(System.getenv("RESULTS_PATH"))
  }
  finally {
    sparkSession.stop()
  }
}
