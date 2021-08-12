import ch.hsr.geohash.GeoHash
import com.opencagedata.geocoder.{OpenCageClient, parts}
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{avg, col, udf}
import org.apache.spark.sql.types.StructType
import structure.{Geocode, Hotel, Weather}

import java.util.Locale
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.io.{BufferedSource, Source}

object SparkApp extends App {

  val azureStorageConfig = getConfig(System.getenv("AZURE_STORAGE_PROPERTIES"))
  val hotelsSchema = ScalaReflection.schemaFor[HotelSchema].dataType.asInstanceOf[StructType]
  val client = new OpenCageClient(System.getenv("OPEN_CAGE_KEY"))

  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Weather by Hotels Provider")
    .config(getAuthConfig(System.getenv("AZURE_ACCOUNT_AUTH_PROPERTIES")))
    .getOrCreate()
  val hotelsDf = sparkSession.read.schema(hotelsSchema).csv(azureStorageConfig("hotels"))
  val weatherDf = sparkSession.read.parquet(azureStorageConfig("weather"))

  try {
    val geoHashUdf = udf(geoHash _)
    val geoCodeUdf = udf(geoCode _)

    val correctHotelRows =
      hotelsDf.filter(col(Hotel.latitude).isNotNull && col(Hotel.longitude).isNotNull)
        .withColumn(Hotel.geohash, geoHashUdf(col(Hotel.latitude), col(Hotel.longitude)))

    val fixedIncorrectHotelRows =
      hotelsDf.filter(col(Hotel.id).isNotNull && (col(Hotel.latitude).isNull || col(Hotel.longitude).isNull))
        .withColumn(Hotel.coordinates, geoCodeUdf(col(Hotel.country), col(Hotel.city), col(Hotel.address)))
        .withColumn(Hotel.latitude, col(Hotel.coordinates).getField(Geocode.latitude))
        .withColumn(Hotel.longitude, col(Hotel.coordinates).getField(Geocode.longitude))
        .withColumn(Hotel.geohash, geoHashUdf(col(Hotel.latitude), col(Hotel.longitude)))
        .drop(Hotel.coordinates)

    val enrichedHotels = correctHotelRows.union(fixedIncorrectHotelRows)

    val enrichedWeather = weatherDf.withColumn(Weather.geohash, geoHashUdf(col(Weather.latitude), col(Weather.longitude)))
      .groupBy(Weather.geohash).agg(avg(col(Weather.avgTempF)).as("avgTempF"), avg(col(Weather.avgTempC)).as("avgTempC"))

    val join = enrichedHotels.join(enrichedWeather, enrichedWeather(Weather.geohash) === enrichedHotels(Hotel.geohash), "left").drop(Weather.geohash)
    val l = join.count()
    join.write.parquet(azureStorageConfig("output.path"))
  }
  finally {
    client.close()
    sparkSession.stop()
  }

  /**
   * Transform property file to SparkConf
   * @param path path to property file
   * @return SparkConf with map of  fs.azure.account.*  properties
   */
  def getAuthConfig(path: String): SparkConf = {
    val configFile: BufferedSource = Source.fromFile(path)
    try {
      new SparkConf().setAll(toKeyValueConfigIterator(configFile).toIterable)
    } finally {
      configFile.close()
    }
  }

  /**
   * Transform property file to Map
   * @param path path to property file
   * @return Map of required Azure properties
   */
  def getConfig(path: String): Map[String, String] = {
    val configFile = Source.fromFile(path)
    try {
        toKeyValueConfigIterator(configFile).toMap
    } finally {
      configFile.close()
    }
  }

  /**
   * Map source file to an iterator of key-value property pairs
   * @param config block of properties to be converted to a map.
   * @return iterator by key-values from source file
   */
  def toKeyValueConfigIterator(config: BufferedSource) = {
    config.getLines.map {
      line =>
        val split = line.split("=")
        split(0) -> split(1)
    }
  }

  /**
   * Get coordinates from Open Cage API based on address.
   * @param country Alpha-2 country code like "US" or "RU"
   * @param city name of the city
   * @param address address line
   * @return parts.LatLong object contains latitude and longitude of an address
   */
  def geoCode(country: String, city: String, address: String): Option[parts.LatLong] = {
    try {
      val responseFuture = client.forwardGeocode(buildProperAddress(country, city, address))
      val response = Await.result(responseFuture, 5.seconds)
      response.results.head.geometry
    } catch {
      case e: Exception => LogManager.getRootLogger.warn("Can't retrieve data from Open Cage: " + e.getMessage)
        null //todo
    }
  }

  /**
   * Get 4-digits geohash based on coordinates.
   * @param lat latitude
   * @param lng longitude
   */
  def geoHash(lat: Double, lng: Double) = {
    GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 4)
  }

  /**
   * Adjust the given address to Open Cage API requirements.
   * The address must contain comma-separated country and city in address line (at least) to find a possible
   * geolocation properly.
   * If input address contains city and country information, we need to be sure that commas
   * are presented as well.
   * If input address is presented in non-readable format, try to build one based on country code and city.
   * @param country Alpha-2 country code like "US" or "RU"
   * @param city name of the city
   * @param address address line
   * @return address line containing a comma-separated city and country
   */
  def buildProperAddress(country: String, city: String, address: String): String = {
    if (address != null && address.contains(city)) {
      if (address.contains(',')) {
        address
      } else {
        address.replace(city, ", " + city + ",")
      }
    } else {
      city + ", " + new Locale("", country).getDisplayCountry
    }
  }
}


//todo write unit tests
//todo cleanup pom.xml
//todo Deploy Spark job on Azure Kubernetes Service (AKS), to setup infrastructure use terraform scripts from module. For this use Running Spark on Kubernetes deployment guide and corresponding to your spark version docker image. Default resource parameters (specifically memory) will not work because of free tier limitations. You needed to setup memory and cores properly.
// todo test if no data multiplication after join
// todo set an OPEN_CAGE_KEY in env