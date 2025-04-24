package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.{SparkSession, functions => F}

object HomeProperty3 {

  def main(args: Array[String]): Unit = {

    val base_filename = "Property_202503.csv"

    Logger.getLogger("org").setLevel(Level.ERROR)
    val logger = Logger.getLogger(this.getClass)

    logger.info("Starting Spark session")
    val spark = SparkSession.builder()
      .appName("Scala Spark Home Property")
      .master("local[*]")
      .config("spark.master", "local")
      .config("spark.driver.memory", "20g")
      .config("spark.executor.memory", "2g")
      .config("spark.driver.memoryOverhead", "4g")
      .config("spark.sql.debug.maxToStringFields", "1000")
      .config("spark.sql.execution.arrow.pyspark.enabled", "true")
      .config("spark.default.parallelism", "16")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("INFO")

    val homePropertyPath = "C:/Users/armen/Desktop/HomeProperty"

    logger.info("Reading input CSV files")

    var dfHomeProperty = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(s"$homePropertyPath/$base_filename")

    logger.info("Read Property_202412 successfully")

    val cleanedDf = dfHomeProperty
      .withColumnRenamed("PROPERTY INDICATOR CODE", "PROPERTY_INDICATOR_CODE")
      .filter(F.col("PROPERTY_INDICATOR_CODE").isNotNull)

    // 5. Get distinct values of PROPERTY_INDICATOR_CODE and process without collect()
    println("Processing each PROPERTY_INDICATOR_CODE value collect()...")

    val distinctValues: Seq[String] = cleanedDf
      .select("PROPERTY_INDICATOR_CODE")
      .distinct()
      .collect()
      .map(_.get(0))                  // extract the value
      .filter(_ != null)              // remove nulls
      .map(_.toString.trim)           // convert to string and trim
      .filter(_.nonEmpty)             // remove empty strings
      .toSeq

    println(distinctValues.mkString("Array(", ", ", ")"))
    distinctValues.foreach { row =>
      try {
        val propertyIndicatorCode = row

        val outputPath = s"$homePropertyPath/property_indicator_codes/property_indicator_code_${propertyIndicatorCode}"
        println(s"Writing data for PROPERTY_INDICATOR_CODE = $propertyIndicatorCode to: $outputPath")

        val filteredDf = cleanedDf.filter(F.col("PROPERTY_INDICATOR_CODE") === propertyIndicatorCode)

        filteredDf.write
          .mode("overwrite")
          .option("header", "true")
          .csv(outputPath)

        println(s"Finished writing data for PROPERTY_INDICATOR_CODE = $propertyIndicatorCode")
      } catch {
        case e: Exception => println(e)
      }
    }

    println("Finished processing all PROPERTY_INDICATOR_CODE values.")

    logger.info("Done.")
    logger.info("Stopping Spark session")
    spark.stop()
  }
}