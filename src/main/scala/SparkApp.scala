import ch.hsr.geohash.GeoHash
import com.opencagedata.geocoder.OpenCageClient
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{avg, col, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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

    val correctRows = hotelsDf.filter(latitude.isNotNull && longitude.isNotNull)
      .withColumn("geohash", geoHashUdf(latitude, longitude))

    val incorrectRows = hotelsDf.filter(latitude.isNull || longitude.isNull)
    val fixedIncorrectRows = incorrectRows.map(row => getGeocode(row))(hotelsDf.encoder)
      .withColumn("geohash", geoHashUdf(latitude, longitude))

    val enrichedHotels = correctRows.union(fixedIncorrectRows)
    val enrichedWeather = weatherDf.withColumn("geohash", geoHashUdf(col("lat"), col("lng")))
    .groupBy("geohash").agg(avg(col("avg_tmpr_f")), avg(col("avg_tmpr_c")))

    val join = enrichedHotels.join(enrichedWeather, enrichedWeather("geohash") === enrichedHotels("geohash"), "left")
//    assert(enrichedHotels.count() == join.count())
    join.show()
  }
  finally {
    client.close()
    sparkSession.stop()
  }

  def geoHash(lat: Double, lng: Double) = {
    GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 4)
  }

  private def getGeocode(row: Row) = {
    val searchAddress = buildProperAddress(row.getAs("country"), row.getAs("city"), row.getAs("address"))
    if (!searchAddress.isBlank) {
      val responseFuture = client.forwardGeocode(searchAddress)
      val response = Await.result(responseFuture, 5.seconds) //todo revise timeout try-catch?
      Row(row(0), row(1), row(2), row(3), row(4), response.results.head.geometry.get.lat, response.results.head.geometry.get.lng) //todo is there any other pretty way to make a replacement? probably using dataset api?
    } else {
      row
    }
  }

  def buildProperAddress(country: String, city: String, address: String): String = {
    if (city.equals("city") && country.equals("country")) {
      return null
    }

    if ((address != null) && address.contains(city)) {
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