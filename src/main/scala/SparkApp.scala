import ch.hsr.geohash.GeoHash
import com.opencagedata.geocoder.{OpenCageClient, parts}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{avg, col, udf}
import org.apache.spark.sql.types.StructType

import java.util.Locale
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.io.Source

object SparkApp extends App {
  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Weather by Hotels Provider")
    .config(getAuthConfig("src\\main\\resources\\config\\azure-account-auth.properties")) //todo to environment variable
    .getOrCreate()

  val azureStorageConfig = getConfig("src\\main\\resources\\config\\azure-storage.properties")

  val hotelsSchema = ScalaReflection.schemaFor[Hotel].dataType.asInstanceOf[StructType]
  val hotelsDf = sparkSession.read.schema(hotelsSchema).csv(azureStorageConfig("hotels"))
  val weatherDf = sparkSession.read.parquet(azureStorageConfig("weather"))
  val client = new OpenCageClient(System.getenv("OPEN_CAGE_KEY"))

  try {
    val latitudeCol = col("latitude")
    val longitudeCol = col("longitude")

    val geoHashUdf = udf(geoHash _)
    val geoCodeUdf = udf(geoCode _)

    val correctRows =
      hotelsDf.filter(latitudeCol.isNotNull && longitudeCol.isNotNull)
        .withColumn("geohash", geoHashUdf(latitudeCol, longitudeCol))

    val fixedIncorrectRows =
      hotelsDf.filter(col("id").isNotNull && (latitudeCol.isNull || longitudeCol.isNull))
        .withColumn("coordinates", geoCodeUdf(col("country"), col("city"), col("address")))
        .withColumn("latitude", col("coordinates").getField("lat"))
        .withColumn("longitude", col("coordinates").getField("lng"))
        .withColumn("geohash", geoHashUdf(latitudeCol, longitudeCol))
        .drop("coordinates")

    val enrichedHotels = correctRows.union(fixedIncorrectRows)
    val enrichedWeather = weatherDf.withColumn("geohash", geoHashUdf(col("lat"), col("lng")))
      .groupBy("geohash").agg(avg(col("avg_tmpr_f")), avg(col("avg_tmpr_c")))

    val join = enrichedHotels.join(enrichedWeather, enrichedWeather("geohash") === enrichedHotels("geohash"), "left")
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