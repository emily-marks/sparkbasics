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

  try {
    val correctRows = df.filter(df.col("latitude").isNotNull && df.col("longitude").isNotNull)
    val incorrectRows = df.filter(df.col("latitude").isNull || df.col("longitude").isNull)
    val fixedCoordinatesRows = incorrectRows.map(row => getGeocode(row))(df.encoder)
  }
  finally {
    client.close()
    sparkSession.stop()
  }


  private def getGeocode(row: Row) = {
    val searchAddress = buildProperAddress(row.getAs("country"), row.getAs("city"), row.getAs("address"))
    if (!searchAddress.isBlank) {
      val responseFuture = client.forwardGeocode(searchAddress)
      val response = Await.result(responseFuture, 5.seconds)
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
