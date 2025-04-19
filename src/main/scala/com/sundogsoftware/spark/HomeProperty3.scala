package com.sundogsoftware.spark

import com.sundogsoftware.spark.AddressStandardizer.standardizeAddress
import org.apache.log4j._
import org.apache.spark.sql.{SparkSession, functions => F}

object HomeProperty3 {

  def main(args: Array[String]): Unit = {

    val base_filename = "Property_202503.csv" // Property_202503_test.csv

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

    dfHomeProperty = dfHomeProperty.na.fill("")

//    dfHomeProperty = dfHomeProperty.withColumn(
//      "situs_city_state_zip_base",
//      F.concat_ws("",
//        F.coalesce(F.col("SITUS CITY"), F.lit("")),
//        F.lit(" "),
//        F.coalesce(F.col("SITUS STATE"), F.lit("")),
//        F.lit(" "),
//        F.coalesce(F.col("SITUS ZIP CODE"), F.lit("")),
//      )
//    )

    // logger.info("standardizeAddress column full_street_name_base")
    // dfHomeProperty = dfHomeProperty.withColumn("full_street_name_base", standardizeAddress(F.col("SITUS STREET ADDRESS")))

    // logger.info("standardizeAddress column situs_city_state_zip_base")
    // dfHomeProperty = dfHomeProperty.withColumn("situs_city_state_zip_base", standardizeAddress(F.col("situs_city_state_zip_base")))

//    dfHomeProperty = dfHomeProperty.withColumn(
//      "full_address_base",
//      F.concat_ws("",
//        F.coalesce(F.col("full_street_name_base"), F.lit("")),
//        F.lit(", "),
//        F.coalesce(F.col("situs_city_state_zip_base"), F.lit("")),
//      )
//    )

//    logger.info("dfHomeProperty filter non-empty")
//    dfHomeProperty = dfHomeProperty
//      .filter(
//        F.col("full_street_name_base").isNotNull && F.trim(F.col("full_street_name_base")) =!= "" &&
//          F.col("ZIP5").isNotNull && F.trim(F.col("ZIP5")) =!= ""
//      )
//
//    logger.info("dfHomeProperty dropDuplicates")
//    dfHomeProperty = dfHomeProperty.dropDuplicates("full_street_name_base", "ZIP5")

    val cleanedDf = dfHomeProperty
      .withColumnRenamed("PROPERTY INDICATOR CODE", "PROPERTY_INDICATOR_CODE")
      .filter(F.col("PROPERTY_INDICATOR_CODE").isNotNull)

    // Use coalesce instead of repartition if reducing number of files
    cleanedDf
      .write
      .partitionBy("PROPERTY_INDICATOR_CODE")
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$homePropertyPath/df_home_base_property_indicator_code")

    logger.info("Done.")
    logger.info("Stopping Spark session")
    spark.stop()
  }
}