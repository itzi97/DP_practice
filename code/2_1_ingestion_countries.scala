import spark.implicits._

case class Country(name: String, iso_code: String, dafif_code: String)

// 1. Read raw text file from the mounted data directory
val countriesRDD = sc.textFile("/data/countries.txt")
countriesRDD.take(5).foreach(println)

// 2. Transform each line into a structured record
val countriesDS = countriesRDD
  .filter(_.nonEmpty)
  .map { line =>
    val parts = line.split(" ## ")
    Country(
      parts(0).split("::")(1).trim,
      parts(1).split("::")(1).trim,
      parts(2).split("::")(1).trim
    )
  }

// 3. Convert to DataFrame
val countriesDf = countriesDS.toDF()
countriesDf.show(5, false)

// 4. Write to HDFS in CSV format
countriesDf.write
  .mode("overwrite")
  .option("header", "true")
  .csv("hdfs://hdfs:9000/practice/countries")

// 5. Read back from HDFS to verify
val countriesHdfsDF = spark.read
  .option("header", "true")
  .csv("hdfs://hdfs:9000/practice/countries")

countriesHdfsDF.show(5, false)
