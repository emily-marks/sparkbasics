import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import schema.WeatherSchema
import structure.Hotel
import tools.WeatherRowsCorrector

class WeatherRowsCorrectorSpec extends AnyFunSuite with Matchers with BeforeAndAfter {

  val inputSchema: StructType = ScalaReflection.schemaFor[WeatherSchema].dataType.asInstanceOf[StructType]

  var spark: SparkSession = _
  var inputWeather: DataFrame = _
  val inputWeatherTest= Seq(
        Row(41.70064,-91.607, 64.5, 18.1, "2016", "2", "1"),
        Row(48.9867,-110.612, 61.9, 16.6, "2016", "2", "1"),
        Row(48.9867,-110.612, 61.1, 16.2, "2016", "2", "1"),
        Row(48.9867,-110.612, 61.9, 16.6, "2016", "3", "5")
      )

  before {
    spark = SparkSession.builder
      .master("local[*]")
      .getOrCreate()
  }

  after {
    spark.close()
  }

  test("geohash is presented and average temperature by ") {
    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputWeatherTest),
      StructType(inputSchema)
    )
    val fixedRows = new WeatherRowsCorrector().avgTempByGeoHash(inputDF)
    fixedRows.count shouldEqual 3 // two rows with the same date are combined
    fixedRows.filter("avgTempF == 61.5").count() shouldEqual 1  //avg temp F
    fixedRows.filter("avgTempC == 16.4").count() shouldEqual 1 // avg temp C
    fixedRows.columns.contains(Hotel.geohash) shouldEqual true
  }
}
