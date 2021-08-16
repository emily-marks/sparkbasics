import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}
import utils.GeoCodeUdf

class BuildAddressSpec extends AnyFunSuite with Matchers with PrivateMethodTester with BeforeAndAfter{

  var country : String = _
  var city : String = _
  var address : String = _
  var geoCodeUdf : GeoCodeUdf = _
  val buildProperAddress: PrivateMethod[String] = PrivateMethod[String]('buildProperAddress)

  before {
    geoCodeUdf = new GeoCodeUdf
  }
  test("build proper address line if address attribute is missing") {
    geoCodeUdf.invokePrivate(buildProperAddress("DE", "Berlin", null)) shouldEqual "Berlin, Germany"
    geoCodeUdf.invokePrivate(buildProperAddress("Germany", "Berlin", null)) should include ("Berlin")
    geoCodeUdf.invokePrivate(buildProperAddress("Not existing country code", "Berlin", null)) should include ("Berlin")
  }

  test ("use comma-separated origin address if it contains city and country in line") {
    geoCodeUdf.invokePrivate(buildProperAddress("DE", "Berlin", "138465 Tiergartenstrasse Berlin Germany")).count(_ == ',') should be >=  2
    geoCodeUdf.invokePrivate(buildProperAddress("DE", "Berlin", "138465 Tiergartenstrasse, Berlin, Germany")) should not include ",,"
  }
}
