import org.mockito.ArgumentMatchers.anyDouble
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import utils.GeohashUdf

class GeoHashSpec extends AnyFunSuite with Matchers{

  test ("geohash should contain 4 symbols") {
    GeohashUdf.geoHash(anyDouble(), anyDouble()).length shouldEqual 4
  }
}
