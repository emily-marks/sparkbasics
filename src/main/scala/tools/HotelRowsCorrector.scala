package tools

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import structure.{Geocode, Hotel}
import utils.{GeoCodeUdf, GeohashUdf}

class HotelRowsCorrector {
  val geoCodeUdf = new GeoCodeUdf()
  /**
   * Multiple steps for correction:
   * - Filter all rows with null latitude or longitude
   * - Retrieve and set new values for latitude/longitude
   * - Build geoHash based on new values
   * @param hotels DataFrame
   * @return enriched hotels DataFrame
   */
  def fixIncorrectRows(hotels: DataFrame): DataFrame = {
    val geoCode = geoCodeUdf.udfGeoCode
    val geoHashUdf = GeohashUdf.udfGeohash
    val a = hotels.filter(col(Hotel.id).isNotNull && (col(Hotel.latitude).isNull || col(Hotel.longitude).isNull))
      .withColumn(Hotel.coordinates, geoCode(col(Hotel.country), col(Hotel.city), col(Hotel.address)))
      .withColumn(Hotel.latitude, col(Hotel.coordinates).getField(Geocode.latitude))
      .withColumn(Hotel.longitude, col(Hotel.coordinates).getField(Geocode.longitude))
      .withColumn(Hotel.geohash, geoHashUdf(col(Hotel.latitude), col(Hotel.longitude)))
      .drop(Hotel.coordinates)
    a
  }

  /**
   * Enrich rows with geohash column.
   * @param hotels origin Dataframe
   * @return Dataframe with geohash
   */
  def addGeohashToCorrectRows(hotels: DataFrame): DataFrame = {
    val geoHashUdf = GeohashUdf.udfGeohash
    hotels.filter(col(Hotel.latitude).isNotNull && col(Hotel.longitude).isNotNull)
      .withColumn(Hotel.geohash, geoHashUdf(col(Hotel.latitude), col(Hotel.longitude)))
  }
}
