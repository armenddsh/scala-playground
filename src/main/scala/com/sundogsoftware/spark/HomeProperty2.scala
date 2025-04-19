package com.sundogsoftware.spark

import com.sundogsoftware.spark.AddressStandardizer._
import org.apache.log4j._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SparkSession, functions => F}

object HomeProperty2 {

  private def createCustomSchema(columns: List[String]): StructType = {
    val fields = columns.map { colName =>
      StructField(colName, StringType, nullable = true) // no backticks!
    }
    StructType(fields)
  }

  private def prefix_columns(prefix: String, long_prefix: String, columns: Array[String]): Array[org.apache.spark.sql.Column] = {
    columns.map(col => F.col(s"$prefix.$col").alias(s"${col}_$long_prefix"))
  }

  def main(args: Array[String]): Unit = {

    val ref_filename = "Ref_202501_202502.csv" //
    val hs_filename = "HS_202501_202502.csv" //
    val base_filename = "Property_202503.csv" // Property_202503_test.csv

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

    var dfHomeProperty = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(s"$homePropertyPath/$base_filename")

    dfHomeProperty = dfHomeProperty.select(
      "APN (PARCEL NUMBER UNFORMATTED)",
      "PROPERTY INDICATOR CODE",
      "SITUS CITY",
      "SITUS STATE",
      "SITUS ZIP CODE",
      "SITUS STREET ADDRESS",
      "ZIP5"
    )
    logger.info("Read Property_202412 successfully")

    // dfHomeProperty = dfHomeProperty.limit(100)

    // logger.info("Property_202412 COUNT: " + dfHomeProperty.count())

    logger.info("Read all input CSV files successfully")

    // SITUS STREET ADDRESS
    // SITUS ZIP CODE -> 360513009 -> 36051 ( split zip into zip5 + zip4 )

    logger.info("Filling null values in DataFrames")

    dfHomeProperty = dfHomeProperty.na.fill("")
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

    dfHomeProperty = dfHomeProperty.withColumn(
      "situs_city_state_zip_base",
      F.concat_ws("",
        F.coalesce(F.col("SITUS CITY"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("SITUS STATE"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("SITUS ZIP CODE"), F.lit("")),
      )
    )

    logger.info("standardizeAddress column full_street_name_base")
    dfHomeProperty = dfHomeProperty.withColumn("full_street_name_base", standardizeAddress(F.col("SITUS STREET ADDRESS")))

    logger.info("standardizeAddress column full_street_name_hs")
    dfHs = dfHs.withColumn("full_street_name_hs", standardizeAddress(F.col("full_street_name_hs")))

    logger.info("standardizeAddress column full_city_state_zip_ref")
    dfRef = dfRef.withColumn("full_city_state_zip_ref", standardizeAddress(F.col("full_city_state_zip_ref")))

    logger.info("standardizeAddress column full_city_state_zip_hs")
    dfHs = dfHs.withColumn("full_city_state_zip_hs", standardizeAddress(F.col("full_city_state_zip_hs")))

    logger.info("standardizeAddress column situs_city_state_zip_base")
    dfHomeProperty = dfHomeProperty.withColumn("situs_city_state_zip_base", standardizeAddress(F.col("situs_city_state_zip_base")))

    logger.info("standardizeAddress column full_street_name_ref")
    dfRef = dfRef.withColumn("full_street_name_ref", standardizeAddress(F.col("full_street_name_ref")))

    dfRef = dfRef.withColumn(
      "full_address_ref",
      F.concat_ws("",
        F.coalesce(F.col("full_street_name_ref"), F.lit("")),
        F.lit(", "),
        F.coalesce(F.col("full_city_state_zip_ref"), F.lit("")),
      )
    )

    dfHs = dfHs.withColumn(
      "full_address_hs",
      F.concat_ws("",
        F.coalesce(F.col("full_street_name_hs"), F.lit("")),
        F.lit(", "),
        F.coalesce(F.col("full_city_state_zip_hs"), F.lit("")),
      )
    )

    dfHomeProperty = dfHomeProperty.withColumn(
      "full_address_base",
      F.concat_ws("",
        F.coalesce(F.col("full_street_name_base"), F.lit("")),
        F.lit(", "),
        F.coalesce(F.col("situs_city_state_zip_base"), F.lit("")),
      )
    )

    logger.info("dfHomeProperty filter non-empty")
    dfHomeProperty = dfHomeProperty
      .filter(
        F.col("full_street_name_base").isNotNull && F.trim(F.col("full_street_name_base")) =!= "" &&
          F.col("ZIP5").isNotNull && F.trim(F.col("ZIP5")) =!= ""
      )

    logger.info("dfHomeProperty dropDuplicates")
    dfHomeProperty = dfHomeProperty.dropDuplicates("full_street_name_base", "ZIP5")

    logger.info("dfHs filter non-empty")
    dfHs = dfHs
      .filter(
        F.col("full_street_name_hs").isNotNull && F.trim(F.col("full_street_name_hs")) =!= "" &&
          F.col("zip").isNotNull && F.trim(F.col("zip")) =!= ""
      )

    logger.info("dfHs dropDuplicates")
    dfHs = dfHs.dropDuplicates("full_street_name_hs", "zip")

    logger.info("dfRef filter non-empty")
    dfRef = dfRef
      .filter(
        F.col("full_street_name_ref").isNotNull && F.trim(F.col("full_street_name_ref")) =!= "" &&
          F.col("zip").isNotNull && F.trim(F.col("zip")) =!= ""
      )

    logger.info("dfRef dropDuplicates")
    dfRef = dfRef.dropDuplicates("full_street_name_ref", "zip")

    val df_ref_columns = prefix_columns("ref", "ref", (dfRef.columns))
    val df_base_columns = prefix_columns("base", "base", (dfHomeProperty.columns))
    val df_hs_columns = prefix_columns("hs", "hs", (dfHs.columns))

    // dfRef.show(truncate = false, numRows = 100)
    // dfHs.show(truncate = false, numRows = 100)
    // dfHomeProperty.show(truncate = false, numRows = 100)

    logger.info("join dfHomeProperty - dfHs")
    val joined_dfHomeProperty_dfHs = dfHomeProperty.alias("base")
      .join(dfHs.alias("hs"),
        F.col("base.ZIP5") === F.col("hs.zip") &&
          F.col("base.full_street_name_base") === F.col("hs.full_street_name_hs")
      )
      .select(df_hs_columns ++ df_base_columns: _*)

    logger.info("join dfHomeProperty - dfRef")

    val joined_dfHomeProperty_dfRef = dfHomeProperty.alias("base")
      .join(dfRef.alias("ref"),
        F.col("base.ZIP5") === F.col("ref.zip") &&
          F.col("base.full_street_name_base") === F.col("ref.full_street_name_ref")
      )
      .select(df_ref_columns ++ df_base_columns: _*)

    logger.info("Saving Home Sales - Home Property joined DataFrame to CSV")
    joined_dfHomeProperty_dfHs
      // .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$homePropertyPath/df_join_hs_home_property")

    logger.info("Saving Refinance - Home Property joined DataFrame to CSV")
    joined_dfHomeProperty_dfRef
      // .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$homePropertyPath/df_join_ref_home_property")

    logger.info("joined_dfHomeProperty_dfHs")
    logger.info(joined_dfHomeProperty_dfHs.count())

    logger.info("joined_dfHomeProperty_dfRef")
    logger.info(joined_dfHomeProperty_dfRef.count())

    // result_dfHomeProperty_dfRef.show(truncate = false, numRows = 100)
    // resultDF_dfHomeProperty_dfHs.show(truncate = false, numRows = 100)

    logger.info("Stopping Spark session")
    spark.stop()
  }
}