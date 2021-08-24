package schema

case class WeatherSchema(
                          lat: Double,
                          lng: Double,
                          avg_tmpr_f: Double,
                          avg_tmpr_c: Double,
                          year: String,
                          month: String,
                          day: String
                        )
