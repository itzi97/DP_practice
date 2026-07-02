import org.apache.spark.sql.functions.udf

val airportsColumns = Seq(
  "airport_id", "name", "city", "country", "iata", "icao",
  "latitude", "longitude", "altitude", "timezone", "dst",
  "tz_database", "type", "source"
)

val airportsDf = spark.read
  .option("header", "false")
  .option("inferSchema", "true")
  .csv("/data/airports.dat")
  .toDF(airportsColumns: _*)

airportsDf.show(5, false)

val quadrantUdf = udf((latitude: Double, longitude: Double) => {
  if (latitude >= 0 && longitude >= 0) "NE"
  else if (latitude >= 0 && longitude < 0) "NW"
  else if (latitude < 0 && longitude >= 0) "SE"
  else "SW"
})

val airportsWithQuadrant = airportsDf.withColumn(
  "hemisphere_quadrant",
  quadrantUdf(airportsDf("latitude"), airportsDf("longitude"))
)

airportsWithQuadrant.show(5, false)

val pgProps = new java.util.Properties()
pgProps.setProperty("user", "spark")
pgProps.setProperty("password", "spark")
pgProps.setProperty("driver", "org.postgresql.Driver")

val pgUrl = "jdbc:postgresql://postgres:5432/sparkdb"

airportsWithQuadrant.write
  .mode("overwrite")
  .jdbc(pgUrl, "airports", pgProps)

spark.read
  .jdbc(pgUrl, "airports", "airport_id", 1, 14000, 4, pgProps)
  .show(5, false)
