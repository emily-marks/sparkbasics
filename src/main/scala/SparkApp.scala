import ch.hsr.geohash.GeoHash
import com.opencagedata.geocoder.{OpenCageClient, parts}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{avg, col, udf}
import org.apache.spark.sql.types.StructType

import java.util.Locale
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object SparkApp extends App {
  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Weather by Hotels Provider")
    .getOrCreate()

  val hotelsSchema = ScalaReflection.schemaFor[Hotel].dataType.asInstanceOf[StructType]
  val hotelsDf = sparkSession.read.schema(hotelsSchema).csv("src/main/resources/hotels/part-00000.csv")
  val weatherDf = sparkSession.read.parquet("src/main/resources/weather/part-00229.parquet")
  val client = new OpenCageClient("")

  try {
    val latitude = col("latitude")
    val longitude = col("longitude")

    val geoHashUdf = udf(geoHash _)
    val geoCodeUdf = udf(geoCode _)

    val correctRows =
      hotelsDf.filter(latitude.isNotNull && longitude.isNotNull)
        .withColumn("geohash", geoHashUdf(latitude, longitude))

    val fixedIncorrectRows =
      hotelsDf.filter(col("id").isNotNull && (latitude.isNull || longitude.isNull))
        .withColumn("coordinates", geoCodeUdf(col("country"), col("city"), col("address")))
        .withColumn("latitude", col("coordinates").getField("lat"))
        .withColumn("longitude", col("coordinates").getField("lng"))
        .withColumn("geohash", geoHashUdf(latitude, longitude))
        .drop("coordinates")

    val enrichedHotels = correctRows.union(fixedIncorrectRows)
    val enrichedWeather = weatherDf.withColumn("geohash", geoHashUdf(col("lat"), col("lng")))
      .groupBy("geohash").agg(avg(col("avg_tmpr_f")), avg(col("avg_tmpr_c")))

    val join = enrichedHotels.join(enrichedWeather, enrichedWeather("geohash") === enrichedHotels("geohash"), "left")
  }
  finally {
    client.close()
    sparkSession.stop()
  }

  def geoCode(country: String, city: String, address: String): Option[parts.LatLong] = {
    val responseFuture = client.forwardGeocode(buildProperAddress(country, city, address))
    val response = Await.result(responseFuture, 5.seconds) //todo revise timeout try-catch?
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


//todo write init tests
//todo mount azure storage
//todo cleanup pom.xml
//todo Deploy Spark job on Azure Kubernetes Service (AKS), to setup infrastructure use terraform scripts from module. For this use Running Spark on Kubernetes deployment guide and corresponding to your spark version docker image. Default resource parameters (specifically memory) will not work because of free tier limitations. You needed to setup memory and cores properly.
// todo sort-merge join?
// todo test if no data multiplication after join