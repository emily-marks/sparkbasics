import ch.hsr.geohash.GeoHash
import com.opencagedata.geocoder.{OpenCageClient, parts}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{avg, col, udf}
import org.apache.spark.sql.types.StructType
import structure.{Geocode, Hotel, Weather}

import java.util.Locale
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.io.Source

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
      .groupBy(Weather.geohash).agg(avg(col(Weather.avgTempF)), avg(col(Weather.avgTempC)))

    val join = enrichedHotels.join(enrichedWeather, enrichedWeather(Weather.geohash) === enrichedHotels(Hotel.geohash), "left")
    //    println("should be 2499 : " + join.count())
  }
  finally {
    client.close()
    sparkSession.stop()
  }

  private def getAuthConfig(path: String): SparkConf = {
    val configFile = Source.fromFile(path)
    try {
      new SparkConf().setAll(
        configFile.getLines.map {
          line =>
            val split = line.split("=")
            split(0) -> split(1)
        }.toIterable)
    } finally {
      configFile.close()
    }
  }

  private def getConfig(path: String): Map[String, String] = {
    val configFile = Source.fromFile(path)
    try {
        configFile.getLines.map {
          line =>
            val split = line.split("=")
            split(0) -> split(1)
        }.toMap
    } finally {
      configFile.close()
    }
  }


  def geoCode(country: String, city: String, address: String): Option[parts.LatLong] = {
    val responseFuture = client.forwardGeocode(buildProperAddress(country, city, address))
    val response = Await.result(responseFuture, 5.seconds)
    response.results.head.geometry
  }

  def geoHash(lat: Double, lng: Double) = {
    GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 4)
  }

  def buildProperAddress(country: String, city: String, address: String): String = {
    if (address != null && address.contains(city)) {
      address.replace(city, ", " + city + ",")
    } else {
      city + ", " + new Locale("", country).getDisplayCountry
    }
  }
}


//todo write unit tests
//todo cleanup pom.xml
//todo Deploy Spark job on Azure Kubernetes Service (AKS), to setup infrastructure use terraform scripts from module. For this use Running Spark on Kubernetes deployment guide and corresponding to your spark version docker image. Default resource parameters (specifically memory) will not work because of free tier limitations. You needed to setup memory and cores properly.
// todo sort-merge join?
// todo test if no data multiplication after join
// todo set an OPEN_CAGE_KEY in env