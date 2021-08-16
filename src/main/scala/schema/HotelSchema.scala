package schema

case class HotelSchema(
                        id: Long,
                        name: String,
                        country: String,
                        city: String,
                        address: String,
                        latitude: Float,
                        longitude: Float
                      )
