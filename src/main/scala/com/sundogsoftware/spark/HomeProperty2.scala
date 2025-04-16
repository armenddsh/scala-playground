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

  def main(args: Array[String]): Unit = {

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

    val homePropertyPath = "D:\\old"

    logger.info("Reading input CSV files")

    logger.info("Reading REF_202501")
    var dfRef = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"$homePropertyPath/REF_202501/REF_202501.csv")
    logger.info("Read REF_202501 successfully")

    logger.info("REF_202501 COUNT: " + dfRef.count())

    logger.info("Reading HS_202501")
    var dfHs = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"$homePropertyPath/HS_202501/HS_202501.csv")
    logger.info("Read HS_202501 successfully")

    logger.info("HS_202501 COUNT: " + dfHs.count())

    val homePropertySchema = createCustomSchema(List(
      "CLIP",
      "FIPS CODE",
      "APN (PARCEL NUMBER UNFORMATTED)",
      "APN SEQUENCE NUMBER",
      "COMPOSITE PROPERTY LINKAGE KEY",
      "ORIGINAL APN",
      "ALTERNATE PARCEL ID",
      "ONLINE FORMATTED PARCEL ID",
      "SITUS HOUSE NUMBER",
      "SITUS HOUSE NUMBER SUFFIX",
      "SITUS HOUSE NUMBER 2",
      "SITUS DIRECTION",
      "SITUS STREET NAME",
      "SITUS MODE",
      "SITUS QUADRANT",
      "SITUS UNIT NUMBER",
      "SITUS CITY",
      "SITUS STATE",
      "SITUS ZIP CODE",
      "SITUS COUNTY",
      "SITUS CARRIER ROUTE",
      "SITUS STREET ADDRESS",
      "SITUS CITY STATE ZIP SOURCE",
      "ZIP5",
      "filedate"
    ))

    var dfHomeProperty = spark.read
      .option("header", "true")
      .schema(homePropertySchema)
      .csv("data/homeproperty/test.csv")

    logger.info("Read Property_202412 successfully")

    logger.info("Property_202412 COUNT: " + dfHomeProperty.count())

    logger.info("Read all input CSV files successfully")
    // Replace recorder with Home Full File

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
      "situs_city_state_zip",
      F.concat_ws("",
        F.coalesce(F.col("SITUS CITY"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("SITUS STATE"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("SITUS ZIP CODE"), F.lit("")),
      )
    )

    logger.info("standardizeAddress column PropertyAddressStreetName")
    dfHomeProperty = dfHomeProperty.withColumn("PropertyAddressStreetName", standardizeAddress(F.col("SITUS STREET ADDRESS")))

    logger.info("standardizeAddress column full_street_name_hs")
    dfHs = dfHs.withColumn("full_street_name_hs", standardizeAddress(F.col("full_street_name_hs")))

    logger.info("standardizeAddress column full_city_state_zip_ref")
    dfRef = dfRef.withColumn("full_city_state_zip_ref", standardizeAddress(F.col("full_city_state_zip_ref")))

    logger.info("standardizeAddress column full_city_state_zip_hs")
    dfHs = dfHs.withColumn("full_city_state_zip_hs", standardizeAddress(F.col("full_city_state_zip_hs")))

    logger.info("standardizeAddress column situs_city_state_zip")
    dfHomeProperty = dfHomeProperty.withColumn("situs_city_state_zip", standardizeAddress(F.col("situs_city_state_zip")))

    logger.info("standardizeAddress column full_street_name_ref")
    dfRef = dfRef.withColumn("full_street_name_ref", standardizeAddress(F.col("full_street_name_ref")))

    logger.info("dfHomeProperty filter non-empty")
    dfHomeProperty = dfHomeProperty
      .filter(
        F.col("SITUS STREET ADDRESS").isNotNull && F.trim(F.col("SITUS STREET ADDRESS")) =!= "" &&
          F.col("ZIP5").isNotNull && F.trim(F.col("ZIP5")) =!= ""
      )

    logger.info("dfHomeProperty dropDuplicates")
    dfHomeProperty = dfHomeProperty.dropDuplicates("SITUS STREET ADDRESS", "ZIP5")

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

    logger.info("join dfHomeProperty - dfHs")
    val joined_dfHomeProperty_dfHs = dfHomeProperty.alias("p")
      .join(dfHs.alias("h"),
        F.col("p.ZIP5") === F.col("h.zip") &&
          F.col("p.SITUS STREET ADDRESS") === F.col("h.full_street_name_hs")
      )

    // Select distinct records with formatted addresses
    val resultDF_dfHomeProperty_dfHs = joined_dfHomeProperty_dfHs.select(
      F.col("p.CLIP"),
      F.col("p.FIPS CODE"),
      F.col("p.APN (PARCEL NUMBER UNFORMATTED)"),
      F.col("p.APN SEQUENCE NUMBER"),
      F.col("p.COMPOSITE PROPERTY LINKAGE KEY"),
      F.col("p.ORIGINAL APN"),
      F.col("p.ONLINE FORMATTED PARCEL ID"),
      F.col("p.ALTERNATE PARCEL ID"),
      F.col("p.SITUS STREET ADDRESS"),
      F.col("p.situs_city_state_zip"),
      F.col("h.full_street_name_hs"),
      F.col("h.full_city_state_zip_hs")
    ).distinct()

    logger.info("join dfHomeProperty - dfRef")

    val joined_dfHomeProperty_dfRef = dfHomeProperty.alias("p")
      .join(dfRef.alias("h"),
        F.col("p.ZIP5") === F.col("h.zip") &&
          F.col("p.SITUS STREET ADDRESS") === F.col("h.full_street_name_ref")
      )

    val result_dfHomeProperty_dfRef = joined_dfHomeProperty_dfRef.select(
      F.col("p.CLIP"),
      F.col("p.FIPS CODE"),
      F.col("p.APN (PARCEL NUMBER UNFORMATTED)"),
      F.col("p.APN SEQUENCE NUMBER"),
      F.col("p.COMPOSITE PROPERTY LINKAGE KEY"),
      F.col("p.ORIGINAL APN"),
      F.col("p.ONLINE FORMATTED PARCEL ID"),
      F.col("p.ALTERNATE PARCEL ID"),
      F.col("p.SITUS STREET ADDRESS"),
      F.col("p.situs_city_state_zip"),
      F.col("h.full_street_name_ref"),
      F.col("h.full_city_state_zip_ref")
    ).distinct()

    logger.info("Saving Home Sales - Recorder joined DataFrame to CSV")
    resultDF_dfHomeProperty_dfHs
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$homePropertyPath/df_join_hs_recorder")

    logger.info("Saving Refinance - Recorder joined DataFrame to CSV")
    result_dfHomeProperty_dfRef
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$homePropertyPath/df_join_ref_rec")

    logger.info("result_dfHomeProperty_dfRef")
    // logger.info(result_dfHomeProperty_dfRef.count())

    logger.info("resultDF_dfHomeProperty_dfHs")
    // logger.info(resultDF_dfHomeProperty_dfHs.count())

    // result_dfHomeProperty_dfRef.show(truncate = false, numRows = 100)
    // resultDF_dfHomeProperty_dfHs.show(truncate = false, numRows = 100)

    logger.info("Stopping Spark session")
    spark.stop()
  }
}