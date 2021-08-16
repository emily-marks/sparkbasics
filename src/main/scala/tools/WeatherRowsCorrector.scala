package tools

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col}
import structure.Weather
import utils.GeohashUdf

class WeatherRowsCorrector {
  /** Create geohash based on lat, lng values and compute average for each geohash
   *
   * @param weather weather Dataframe
   * @return weather Dataframe grouped by geohash
   */
  def avgTempByGeoHash(weather: DataFrame): DataFrame = {
    val geohashUdf = GeohashUdf.udfGeohash
    weather.withColumn(Weather.geohash, geohashUdf(col(Weather.latitude), col(Weather.longitude)))
      .groupBy(Weather.year, Weather.month, Weather.day, Weather.geohash).agg(avg(col(Weather.avgTempF)).as("avgTempF"), avg(col(Weather.avgTempC)).as("avgTempC"))
  }
}
