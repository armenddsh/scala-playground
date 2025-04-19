package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.{SparkSession, functions => F}

object HomeProperty3 {

  def main(args: Array[String]): Unit = {

    val base_filename = "Property_202503_test.csv" // Property_202503_test.csv

    Logger.getLogger("org").setLevel(Level.ERROR)
    val logger = Logger.getLogger(this.getClass)

    logger.info("Starting Spark session")
    val spark = SparkSession.builder()
      .appName("Scala Spark Home Property")
      .master("local[*]")
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

    dfHomeProperty = dfHomeProperty.select(
      "APN (PARCEL NUMBER UNFORMATTED)",
      "PROPERTY INDICATOR CODE",
      "LAND USE CODE",
      "SITUS CITY",
      "SITUS STATE",
      "SITUS ZIP CODE",
      "SITUS STREET ADDRESS",
      "ZIP5"
    )

    logger.info("Read Property_202412 successfully")

    dfHomeProperty = dfHomeProperty.withColumnRenamed("PROPERTY INDICATOR CODE","PROPERTY_INDICATOR_CODE")

    dfHomeProperty
      .repartition(F.col("PROPERTY_INDICATOR_CODE"))
      .write
      .partitionBy("PROPERTY_INDICATOR_CODE")
      .mode("overwrite")
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$homePropertyPath/df_home_base_property_indicator_code")

    logger.info("Done.")
    logger.info("Stopping Spark session")
    spark.stop()
  }
}