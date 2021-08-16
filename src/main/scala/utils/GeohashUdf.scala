package utils

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/**
 * Provides methods for udf
 */
object GeohashUdf {

  def udfGeohash: UserDefinedFunction = udf(geoHash _)
  /**
  * Get 4-digits geohash based on coordinates.
    * @param lat latitude
  * @param lng longitude
  */
  def geoHash(lat: Double, lng: Double): String = {
    GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 4)
  }
}
