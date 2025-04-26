package com.sundogsoftware.spark

import com.sundogsoftware.spark.AddressStandardizer._
import org.apache.log4j._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SparkSession, functions => F}

object HomeProperty5 {

  private def prefix_columns(prefix: String, long_prefix: String, columns: Array[String]): Array[org.apache.spark.sql.Column] = {
    columns.map(col => F.col(s"$prefix.$col").alias(s"${col}_$long_prefix"))
  }

  def main(args: Array[String]): Unit = {

    val ref_filename = "Ref_202501_202502.csv" //
    val hs_filename = "HS_202501_202502.csv" //

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

    logger.info("Reading REF_202501")
    var dfRef = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(s"$homePropertyPath/$ref_filename")

    logger.info("Read REF_202501 successfully")

    dfRef = dfRef.select(
      "street_number",
      "direction_one",
      "street",
      "street_type",
      "direction_two",
      "suite",
      "city",
      "state",
      "zip"
    )

    logger.info("REF_202501 COUNT: " + dfRef.count())

    logger.info("Reading HS_202501")
    var dfHs = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(s"$homePropertyPath/$hs_filename")

    logger.info("Read HS_202501 successfully")
    dfHs = dfHs.select(
      "street_number",
      "direction_one",
      "street",
      "street_type",
      "direction_two",
      "suite",
      "city",
      "state",
      "zip"
    )

    logger.info("HS_202501 COUNT: " + dfHs.count())

    dfRef = dfRef.na.fill("")
    dfHs = dfHs.na.fill("")

    logger.info("Filling null values in DataFrames Done")

    logger.info("Creating column full_street_name_hs")
    dfHs = dfHs.withColumn(
      "full_street_name_hs",
      F.concat_ws("",
        F.coalesce(F.col("street_number"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("direction_one"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("street"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("street_type"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("direction_two"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("suite"), F.lit(""))
      )
    )

    dfHs = dfHs.withColumn(
      "full_city_state_zip_hs",
      F.concat_ws("",
        F.coalesce(F.col("city"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("state"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("zip"), F.lit("")),
      )
    )

    logger.info("Creating column full_street_name_ref")
    dfRef = dfRef.withColumn(
      "full_street_name_ref",
      F.concat_ws("",
        F.coalesce(F.col("street_number"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("direction_one"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("street"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("street_type"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("direction_two"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("suite"), F.lit(""))
      )
    )

    dfRef = dfRef.withColumn(
      "full_city_state_zip_ref",
      F.concat_ws("",
        F.coalesce(F.col("city"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("state"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("zip"), F.lit("")),
      )
    )

    logger.info("standardizeAddress column full_street_name_hs")
    // dfHs = standardizeAddress(dfHs, "full_street_name_hs", "full_street_name_hs_standardized")

    logger.info("standardizeAddress column full_street_name_ref")
    // dfRef = standardizeAddress(dfRef, "full_street_name_ref", "full_street_name_ref_standardized")

    logger.info("dfHs filter non-empty")
    dfHs = dfHs
      .filter(
        F.col("full_street_name_hs_standardized").isNotNull && F.trim(F.col("full_street_name_hs_standardized")) =!= "" &&
          F.col("zip").isNotNull && F.trim(F.col("zip")) =!= ""
      )

    logger.info("dfHs dropDuplicates")
    dfHs = dfHs.dropDuplicates("full_street_name_hs", "zip")

    logger.info("dfRef filter non-empty")
    dfRef = dfRef
      .filter(
        F.col("full_street_name_ref_standardized").isNotNull && F.trim(F.col("full_street_name_ref_standardized")) =!= "" &&
          F.col("zip").isNotNull && F.trim(F.col("zip")) =!= ""
      )

    logger.info("dfRef dropDuplicates")
    dfRef = dfRef.dropDuplicates("full_street_name_ref_standardized", "zip")

    logger.info("join dfHomeProperty - dfRef")

    logger.info("Saving Home Sales - Home Property joined DataFrame to CSV")
    dfHs
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$homePropertyPath/df_join_hs_home_property_v2")

    logger.info("Saving Refinance - Home Property joined DataFrame to CSV")
    dfRef
      .coalesce(1 )
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$homePropertyPath/df_join_ref_home_property_v2")

    // logger.info("joined_dfHomeProperty_dfHs")
    // logger.info(joined_dfHomeProperty_dfHs.count())

    // logger.info("joined_dfHomeProperty_dfRef")
    // logger.info(joined_dfHomeProperty_dfRef.count())

    // result_dfHomeProperty_dfRef.show(truncate = false, numRows = 100)
    // resultDF_dfHomeProperty_dfHs.show(truncate = false, numRows = 100)

    logger.info("Stopping Spark session")
    spark.stop()
  }
}