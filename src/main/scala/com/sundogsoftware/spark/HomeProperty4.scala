package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession, functions => F}

object HomeProperty4 {

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

    val dfHomeProperty = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(s"$homePropertyPath/$base_filename")

    logger.info("Read Property_202412 successfully")

    var cleanedDf = dfHomeProperty
      .withColumnRenamed("PROPERTY INDICATOR CODE", "PROPERTY_INDICATOR_CODE")

    println("Processing each PROPERTY_INDICATOR_CODE value collect()...")

    cleanedDf = cleanedDf
      .select("PROPERTY_INDICATOR_CODE")
      .filter(F.col("PROPERTY_INDICATOR_CODE").isNotNull)
      .dropDuplicates()

    val outputPath = s"$homePropertyPath/property_indicator_codes/property_indicator_code_unique"

    cleanedDf
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)

    println("Finished processing all PROPERTY_INDICATOR_CODE values.")

    logger.info("Done.")
    logger.info("Stopping Spark session")
    spark.stop()
  }
}