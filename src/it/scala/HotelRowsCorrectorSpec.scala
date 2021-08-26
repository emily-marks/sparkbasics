import com.opencagedata.geocoder.parts
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import schema.HotelSchema
import structure.Hotel
import tools.HotelRowsCorrector
import utils.GeoCodeUdf

class HotelRowsCorrectorSpec extends AnyFunSuite with Matchers with BeforeAndAfter {

  val testLat = 64
  val testLong = 110
  val inputSchema: StructType = ScalaReflection.schemaFor[HotelSchema].dataType.asInstanceOf[StructType]

  var spark: SparkSession = _
  var inputDFWithIncorrectRow: DataFrame = _
  var inputDFWithCorrectRow: DataFrame = _


  before {
    spark = SparkSession.builder
      .master("local[*]")
      .getOrCreate()
    inputDFWithIncorrectRow = spark.read.schema(inputSchema).csv("src/it/resources/incorrect_hotels.csv")
    inputDFWithCorrectRow = spark.read.schema(inputSchema).csv("src/it/resources/correct_hotels.csv")
  }

  after {
    spark.close()
  }

  test("filter incorrect rows and fix them + geohash column") {

    val fixedRows = new HotelRowsCorrectorTest().fixIncorrectRows(inputDFWithIncorrectRow)
    fixedRows.count shouldEqual 1
    fixedRows.columns.contains(Hotel.geohash) shouldEqual true
    fixedRows.columns.contains(Hotel.coordinates) shouldEqual false
  }

  test("no incorrect values in DF") {

    val fixedRows = new HotelRowsCorrectorTest().fixIncorrectRows(inputDFWithCorrectRow)
    fixedRows.count shouldEqual 0
  }

  test("enrich correct data with geohash") {
    val fixedRows = new HotelRowsCorrectorTest().addGeohashToCorrectRows(inputDFWithCorrectRow)
    fixedRows.count shouldEqual 1
    fixedRows.columns.contains(Hotel.geohash) shouldEqual true
    fixedRows.select(Hotel.geohash) should not be null
  }

  test("enrich correct data with geohash if incorrect data is presented as well") {
    val fixedRows = new HotelRowsCorrectorTest().addGeohashToCorrectRows(inputDFWithIncorrectRow)
    fixedRows.count shouldEqual 1
    fixedRows.columns.contains(Hotel.geohash) shouldEqual true
    fixedRows.select(Hotel.geohash) should not be null
  }

  class HotelRowsCorrectorTest extends HotelRowsCorrector{
    override val geoCodeUdf: GeoCodeUdf = new GeoCodeUdf {
      override def udfGeoCode: UserDefinedFunction =
        udf((_: String, _: String, _: String) => parts.LatLong(testLat, testLong))
    }
  }
}
