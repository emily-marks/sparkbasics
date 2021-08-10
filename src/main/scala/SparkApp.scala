import SparkApp.df
import com.opencagedata.geocoder.OpenCageClient
import org.apache.spark.sql.{Column, DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType

import java.util.Locale
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object SparkApp extends App {
  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Azure Storage Reader")
    .getOrCreate()
  val schema = ScalaReflection.schemaFor[Hotel].dataType.asInstanceOf[StructType]
  val df: DataFrame = sparkSession.read.schema(schema).csv("src/main/resources/part-00000.csv")
  val client: OpenCageClient = new OpenCageClient("")

//  df.filter(df.col("latitude").isNull || df.col("longitude").isNull)
//    .withColumn("response", lit(getGeocode1(df("country")., df("city"), df("address")))).cache
//    .withColumn("latitude", col("response"))
//    .withColumn("longitude", lit())
//    .drop("response")
//    .show()
  try {
    //    val emptyCoordinatesAddress =
    val fixedRows = df.filter(df.col("latitude").isNull || df.col("longitude").isNull)
      //        .select("city")
      .map(row => getGeocode(row))(df.encoder).cache()
//      .toDF()
      .show
//      .show
//    val resultDf = if (cond) {
//
//    } else {
//
//    }



  df.filter(df.col("latitude").isNull || df.col("longitude").isNull).show(false)

//  df.intersect(fixedRows)
  //  client.close()
  }
  finally {
    client.close()
  }
  sparkSession.stop()

  private def getGeocode1(country: Column, city: Column, address: Column) = {
    val searchAddress = getProperAddress1(country, city, address)
//    if (!searchAddress.isBlank) {
      val responseFuture = client.forwardGeocode(searchAddress)
      val response = Await.result(responseFuture, 5.seconds)

      //          if (response.status.code == 200 & response.results.isEmpty) {
      //             row
      //          }

      //          println(response.results(0).geometry)
       response.results.head.geometry
    "12312" //todo
//    } else {
//      row
//    }
  }

  private def getGeocode(row: Row) = {
    val searchAddress = getProperAddress(row.getAs("country"), row.getAs("city"), row.getAs("address"))
    if (!searchAddress.isBlank) {
      val responseFuture = client.forwardGeocode(searchAddress)
      val response = Await.result(responseFuture, 5.seconds)

      //          if (response.status.code == 200 & response.results.isEmpty) {
      //             row
      //          }

      //          println(response.results(0).geometry)
      Row(row(0), row(1), row(2), row(3), row(4), response.results.head.geometry.get.lat, response.results.head.geometry.get.lng) //todo is there any other pretty way to make a replacement? probably using dataset api?
    } else {
      row
    }
  }

  def getProperAddress1(country: Column, city: Column, address: Column): String = {
address.contains(city)
"lalala"
    //    if ((address != null) && address.contains(city)) {
//      address.replace(city, ", " + city + ",")
//      //      address
//    } else {
//      city + ", " + new Locale("", country).getDisplayCountry
//    }
  }

  def getProperAddress(country: String, city: String, address: String): String = {
    //    if (city.equals("city") && country.equals("country")) {
    //      return null;
    //    }
    //
    if ((address != null) && address.contains(city)) {
      address.replace(city, ", " + city + ",")
      //      address
    } else {
      city + ", " + new Locale("", country).getDisplayCountry
    }
  }
}
