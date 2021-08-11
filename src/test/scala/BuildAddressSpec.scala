import org.mockito.ArgumentMatchers.anyDouble
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BuildAddressSpec extends AnyFunSuite with Matchers{

  test("build proper address line if address attribute is missing") {
    SparkApp.buildProperAddress("DE", "Berlin", null) shouldEqual "Berlin, Germany"
    SparkApp.buildProperAddress("Germany", "Berlin", null) should include ("Berlin")
    SparkApp.buildProperAddress("Not existing country code", "Berlin", null) should include ("Berlin")
  }

  test ("use comma-separated origin address if it contains city and country in line") {
    SparkApp.buildProperAddress("DE", "Berlin", "138465 Tiergartenstrasse Berlin Germany").count(_ == ',') should be >=  2
    SparkApp.buildProperAddress("DE", "Berlin", "138465 Tiergartenstrasse, Berlin, Germany") should not include ",,"
  }
}
