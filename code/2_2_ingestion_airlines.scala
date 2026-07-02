import spark.implicits._
import org.apache.spark.sql.functions._

val airlinesDF = spark.read
  .option("header", "false")
  .option("inferSchema", "true")
  .csv("/data/airlines.dat")
  .toDF("airline_id", "name", "alias", "iata", "icao", "callsign", "country", "active")

airlinesDF.printSchema()
airlinesDF.show(5, false)

val airlinesClean = airlinesDF
  .withColumn("active", when(upper(col("active")) === "Y", 1).otherwise(0))
  .filter(col("active") === 1)

airlinesClean.printSchema()
airlinesClean.show(5, false)

spark.sql("CREATE DATABASE IF NOT EXISTS practice")
spark.sql("CREATE DATABASE IF NOT EXISTS practice")

val path = new org.apache.hadoop.fs.Path("hdfs://hdfs:9000/practice/airlines")

airlinesClean.write
  .mode("overwrite")
  .partitionBy("country")
  .parquet("hdfs://hdfs:9000/practice/airlines")

val airlinesHdfsDF = spark.read
  .parquet("hdfs://hdfs:9000/practice/airlines")

airlinesHdfsDF.show(5, false)
airlinesHdfsDF.printSchema()
