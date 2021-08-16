package utils

import com.opencagedata.geocoder.{OpenCageClient, parts}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.util.Locale
import java.util.logging.Logger
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

/**
 * Provides methods for udf
 */
class GeoCodeUdf extends Serializable {

  def udfGeoCode: UserDefinedFunction = udf(geoCode _)

  /**
   * Get coordinates from Open Cage API based on address.
   * @param country Alpha-2 country code like "US" or "RU"
   * @param city name of the city
   * @param address address line
   * @return parts.LatLong object contains latitude and longitude of an address
   */
  def geoCode(country: String, city: String, address: String): Option[parts.LatLong] = {
    val client = new OpenCageClient(System.getenv("OPEN_CAGE_KEY"))
    try {
      val responseFuture = client.forwardGeocode(buildProperAddress(country, city, address))
      val response = Await.result(responseFuture, 5.seconds)
      response.results.head.geometry
    } catch {
      case e: Exception => Logger.getLogger("OpenCageClient").warning("Open cage issue" + e.getMessage)
        null
    } finally {
      client.close()
    }
  }

  /**
   * Adjust the given address to Open Cage API requirements.
   * The address must contain comma-separated country and city in address line (at least) to find a possible
   * geolocation properly.
   * If input address contains city and country information, we need to be sure that commas
   * are presented as well.
   * If input address is presented in non-readable format, try to build one based on country code and city.
   *
   * @param country Alpha-2 country code like "US" or "RU"
   * @param city    name of the city
   * @param address address line
   * @return address line containing a comma-separated city and country
   */
  private def buildProperAddress(country: String, city: String, address: String): String = {
    if (address != null && address.contains(city)) {
      if (address.contains(',')) {
        address
      } else {
        address.replace(city, ", " + city + ",")
      }
    } else {
      city + ", " + new Locale("", country).getDisplayCountry
    }
  }
}
