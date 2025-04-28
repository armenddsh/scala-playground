package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.storage.StorageLevel

object HomeProperty6 {

  def main(args: Array[String]): Unit = {

    val ref_filename = "Ref_202501_202502.csv" //
    val hs_filename = "HS_202501_202502.csv" //
    val base_filename = "Property_202503.csv" // Property_202503_test.csv

    Logger.getLogger("org").setLevel(Level.ERROR)
    val logger = Logger.getLogger(this.getClass)

    val isRemote = false
    val remoteDir = "/home/sshadmin/files"

    val homePropertyPath = "C:/Users/armen/Desktop/HomeProperty"
    var dir = ""
    var tempDir = ""

    if (isRemote) {
      dir = remoteDir
      tempDir = "/home/sshadmin/spark-temp"
    } else {
      dir = homePropertyPath
      tempDir = "C:/temp"
    }

    logger.info("Starting Spark session")
    val spark = SparkSession.builder()
      .appName("Scala Spark Home Property")
      .master("local[*]")
      .config("spark.local.dir", tempDir)
      .config("spark.driver.memory", "100g")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memoryOverhead", "8g")
      .config("spark.sql.debug.maxToStringFields", "1000")
      .config("spark.sql.execution.arrow.pyspark.enabled", "true")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("INFO")
    
    logger.info("Reading input CSV files")

    logger.info("Reading REF_202501")

    val dfRefCsv = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(s"$dir/$ref_filename")

    var dfRef = dfRefCsv.select(
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

    val c1Df = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(s"$dir/C1.csv")

    logger.info("REF_202501 COUNT: " + dfRef.count())

    logger.info("Reading HS_202501")
    val dfHsCsv = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(s"$dir/$hs_filename")

    var dfHs = dfHsCsv.select(
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

    logger.info("Read HS_202501 successfully")

    val dfHomePropertyCsv = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(s"$dir/$base_filename")

    var dfHomeProperty = dfHomePropertyCsv.select(
      "APN (PARCEL NUMBER UNFORMATTED)",
      "LAND USE CODE",
      "PROPERTY INDICATOR CODE",
      "SITUS HOUSE NUMBER",
      "SITUS HOUSE NUMBER SUFFIX",
      "SITUS HOUSE NUMBER 2",
      "SITUS DIRECTION",
      "SITUS STREET NAME",
      "SITUS MODE",
      "SITUS QUADRANT",
      "SITUS UNIT NUMBER",
      "SITUS STREET ADDRESS",
      "SITUS CITY",
      "SITUS STATE",
      "SITUS ZIP CODE",
      "ZIP5",
      "SITUS COUNTY"
    )

    logger.info("Read all input CSV files successfully")

    logger.info("Filling null values in DataFrames")

    dfHomeProperty = dfHomeProperty.na.fill("")
    dfRef = dfRef.na.fill("")
    dfHs = dfHs.na.fill("")

    dfHs = StringCleaner.cleanColumn(dfHs, "street_type", "street_type")
    dfRef = StringCleaner.cleanColumn(dfRef, "street_type", "street_type")
    dfHomeProperty = StringCleaner.cleanColumn(dfHomeProperty, "SITUS MODE", "SITUS MODE")

    var join_street_type_hs_c1 = dfHs.join(
      c1Df,
      upper(dfHs("street_type")) === upper(c1Df("street_type_c1")),
      "left"
    )

    var join_street_type_ref_c1 = dfRef.join(
      c1Df,
      upper(dfRef("street_type")) === upper(c1Df("street_type_c1")),
      "left"
    )

    var join_street_type_base_c1 = dfHomeProperty.join(
      c1Df,
      upper(dfHomeProperty("SITUS MODE")) === upper(c1Df("street_type_c1")),
      "left"
    )

    logger.info("Creating column full_street_name_hs")

    join_street_type_hs_c1 = join_street_type_hs_c1.withColumn(
      "full_street_name_hs",
      upper(F.concat_ws("",
        F.coalesce(F.col("street_number"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("direction_one"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("street"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("STD_street_type_c1"), F.col("street_type"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("direction_two"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("suite"), F.lit(""))
      ))
    )

    join_street_type_hs_c1 = join_street_type_hs_c1.withColumn(
      "full_street_name_hs_no_suite",
      upper(F.concat_ws("",
        F.coalesce(F.col("street_number"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("direction_one"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("street"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("STD_street_type_c1"), F.col("street_type"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("direction_two"), F.lit("")),
      ))
    )

    logger.info("Creating column full_street_name_ref")

    join_street_type_ref_c1 = join_street_type_ref_c1.withColumn(
      "full_street_name_ref",
      upper(F.concat_ws("",
        F.coalesce(F.col("street_number"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("direction_one"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("street"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("STD_street_type_c1"), F.col("street_type"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("direction_two"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("suite"), F.lit(""))
      ))
    )

    join_street_type_ref_c1 = join_street_type_ref_c1.withColumn(
      "full_street_name_ref_no_suite",
      upper(F.concat_ws("",
        F.coalesce(F.col("street_number"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("direction_one"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("street"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("STD_street_type_c1"), F.col("street_type"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("direction_two"), F.lit("")),
      ))
    )

    join_street_type_base_c1 = join_street_type_base_c1.withColumn(
      "full_street_name_base",
      upper(F.concat_ws("",
        F.coalesce(F.col("SITUS HOUSE NUMBER"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("SITUS DIRECTION"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("SITUS STREET NAME"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("STD_street_type_c1"), F.col("SITUS MODE"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("SITUS QUADRANT"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("SITUS UNIT NUMBER"), F.lit(""))
      ))
    )

    join_street_type_base_c1 = join_street_type_base_c1.withColumn(
      "full_street_name_base_no_suite",
      upper(F.concat_ws("",
        F.coalesce(F.col("SITUS HOUSE NUMBER"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("SITUS DIRECTION"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("SITUS STREET NAME"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("STD_street_type_c1"), F.col("SITUS MODE"), F.lit("")),
        F.lit(" "),
        F.coalesce(F.col("SITUS QUADRANT"), F.lit("")),
      ))
    )

    join_street_type_base_c1 = StringCleaner
      .cleanColumn(join_street_type_base_c1, "full_street_name_base", "full_street_name_base")

    join_street_type_ref_c1 = StringCleaner
      .cleanColumn(join_street_type_ref_c1, "full_street_name_ref", "full_street_name_ref")

    join_street_type_hs_c1 = StringCleaner
      .cleanColumn(join_street_type_hs_c1, "full_street_name_hs", "full_street_name_hs")

    join_street_type_base_c1 = join_street_type_base_c1
      .filter(
        F.col("full_street_name_base").isNotNull && F.trim(F.col("full_street_name_base")) =!= "" &&
          F.col("ZIP5").isNotNull && F.trim(F.col("ZIP5")) =!= ""
      )

    logger.info("dfHomeProperty dropDuplicates")
    join_street_type_base_c1 = join_street_type_base_c1.dropDuplicates("full_street_name_base", "ZIP5")

    logger.info("dfHs filter non-empty")
    join_street_type_hs_c1 = join_street_type_hs_c1
      .filter(
        F.col("full_street_name_hs").isNotNull && F.trim(F.col("full_street_name_hs")) =!= "" &&
          F.col("zip").isNotNull && F.trim(F.col("zip")) =!= ""
      )

    logger.info("dfHs dropDuplicates")
    join_street_type_hs_c1 = join_street_type_hs_c1.dropDuplicates("full_street_name_hs", "zip")

    logger.info("dfRef filter non-empty")
    join_street_type_ref_c1 = join_street_type_ref_c1
      .filter(
        F.col("full_street_name_ref").isNotNull && F.trim(F.col("full_street_name_ref")) =!= "" &&
          F.col("zip").isNotNull && F.trim(F.col("zip")) =!= ""
      )

    logger.info("dfRef dropDuplicates")
    join_street_type_ref_c1 = join_street_type_ref_c1.dropDuplicates("full_street_name_ref", "zip")

    join_street_type_base_c1 = StringCleaner
      .cleanColumn(join_street_type_base_c1, "full_street_name_base_no_suite", "full_street_name_base_no_suite")

    join_street_type_ref_c1 = StringCleaner
      .cleanColumn(join_street_type_ref_c1, "full_street_name_ref_no_suite", "full_street_name_ref_no_suite")

    join_street_type_hs_c1 = StringCleaner
      .cleanColumn(join_street_type_hs_c1, "full_street_name_hs_no_suite", "full_street_name_hs_no_suite")

    join_street_type_base_c1 = join_street_type_base_c1
      .filter(
        F.col("full_street_name_base_no_suite").isNotNull && F.trim(F.col("full_street_name_base_no_suite")) =!= "" &&
          F.col("ZIP5").isNotNull && F.trim(F.col("ZIP5")) =!= ""
      )

    logger.info("dfHs filter non-empty")
    join_street_type_hs_c1 = join_street_type_hs_c1
      .filter(
        F.col("full_street_name_hs_no_suite").isNotNull && F.trim(F.col("full_street_name_hs_no_suite")) =!= "" &&
          F.col("zip").isNotNull && F.trim(F.col("zip")) =!= ""
      )


    logger.info("dfRef filter non-empty")
    join_street_type_ref_c1 = join_street_type_ref_c1
      .filter(
        F.col("full_street_name_ref_no_suite").isNotNull && F.trim(F.col("full_street_name_ref_no_suite")) =!= "" &&
          F.col("zip").isNotNull && F.trim(F.col("zip")) =!= ""
      )

    val df_ref_columns = Utils.prefix_columns("ref", "ref", join_street_type_ref_c1.columns)
    val df_base_columns = Utils.prefix_columns("base", "base", join_street_type_base_c1.columns)
    val df_hs_columns = Utils.prefix_columns("hs", "hs", join_street_type_hs_c1.columns)

    logger.info("join dfHomeProperty - dfHs")

    join_street_type_base_c1 = join_street_type_base_c1.persist(StorageLevel.MEMORY_AND_DISK)
    join_street_type_hs_c1 = join_street_type_hs_c1.persist(StorageLevel.MEMORY_AND_DISK)
    join_street_type_ref_c1 = join_street_type_ref_c1.persist(StorageLevel.MEMORY_AND_DISK)

    var joined_dfHomeProperty_dfHs = join_street_type_base_c1.alias("base")
      .join(join_street_type_hs_c1.alias("hs"),
        col("base.ZIP5") === col("hs.zip") &&
          (
            col("base.full_street_name_base") === col("hs.full_street_name_hs") ||
              col("base.`SITUS STREET ADDRESS`") === col("hs.full_street_name_hs") ||
              col("base.`SITUS STREET ADDRESS`") === col("hs.full_street_name_hs_no_suite")
            ),
        "inner"
      )
      .select(df_hs_columns ++ df_base_columns: _*)

    logger.info("join dfHomeProperty - dfRef")

    var joined_dfHomeProperty_dfRef = join_street_type_base_c1.alias("base")
      .join(join_street_type_ref_c1.alias("ref"),
        col("base.ZIP5") === col("ref.zip") &&
          (
            col("base.full_street_name_base") === col("ref.full_street_name_ref") ||
              col("base.`SITUS STREET ADDRESS`") === col("ref.full_street_name_ref") ||
              col("base.`SITUS STREET ADDRESS`") === col("ref.full_street_name_ref_no_suite")
            ),
        "inner"
      )
      .select(df_ref_columns ++ df_base_columns: _*)

    var joined_dfHomeProperty_dfHs_not_matched = join_street_type_hs_c1.alias("hs")
      .join(join_street_type_base_c1.alias("base"),
        col("base.ZIP5") === col("hs.zip") &&
          (
            col("base.full_street_name_base") === col("hs.full_street_name_hs") ||
              col("base.`SITUS STREET ADDRESS`") === col("hs.full_street_name_hs") ||
              col("base.`SITUS STREET ADDRESS`") === col("hs.full_street_name_hs_no_suite")
            ),
        "leftanti"
      )

    logger.info("join dfHomeProperty - dfRef")

    var joined_dfHomeProperty_dfRef_not_matched = join_street_type_ref_c1.alias("ref")
      .join(join_street_type_base_c1.alias("base"),
        col("base.ZIP5") === col("ref.zip") &&
          (
            col("base.full_street_name_base") === col("ref.full_street_name_ref") ||
              col("base.`SITUS STREET ADDRESS`") === col("ref.full_street_name_ref") ||
              col("base.`SITUS STREET ADDRESS`") === col("ref.full_street_name_ref_no_suite")
            ),
        "leftanti"
      )

    // joined_dfHomeProperty_dfHs = joined_dfHomeProperty_dfHs.persist(StorageLevel.MEMORY_AND_DISK)
    // joined_dfHomeProperty_dfRef = joined_dfHomeProperty_dfRef.persist(StorageLevel.MEMORY_AND_DISK)
    // joined_dfHomeProperty_dfHs_not_matched = joined_dfHomeProperty_dfHs_not_matched.persist(StorageLevel.MEMORY_AND_DISK)
    // joined_dfHomeProperty_dfRef_not_matched = joined_dfHomeProperty_dfRef_not_matched.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info("Saving Home Sales - Home Property joined DataFrame to CSV")
    joined_dfHomeProperty_dfHs
      .na.fill("")
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$dir/df_join_hs_home_property_4_matched")

    logger.info("Saving Refinance - Home Property joined DataFrame to CSV")
    joined_dfHomeProperty_dfRef
      .na.fill("")
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$dir/df_join_ref_home_property_4_matched")

    logger.info("Saving Home Sales - Home Property joined DataFrame Not matched to CSV")

//    joined_dfHomeProperty_dfHs_not_matched
//      .na.fill("")
//      .write
//      .option("header", "true")
//      .mode("overwrite")
//      .csv(s"$dir/df_join_hs_home_property_4_not_matched")
//
//    logger.info("Saving Ref - Home Property joined Not matched DataFrame to CSV")
//
//    joined_dfHomeProperty_dfRef_not_matched
//      .na.fill("")
//      .write
//      .option("header", "true")
//      .mode("overwrite")
//      .csv(s"$dir/df_join_ref_home_property_4_not_matched")

//    logger.info("joined_dfHomeProperty_dfHs.count()")
//    logger.info(joined_dfHomeProperty_dfHs.count())
//
//    logger.info("joined_dfHomeProperty_dfRef")
//    logger.info(joined_dfHomeProperty_dfRef.count())
//
//    logger.info("joined_dfHomeProperty_dfHs_not_matched.count()")
//    logger.info(joined_dfHomeProperty_dfHs_not_matched.count())
//
//    logger.info("joined_dfHomeProperty_dfRef_not_matched.count()")
//    logger.info(joined_dfHomeProperty_dfRef_not_matched.count())

    logger.info("Stopping Spark session")
    spark.stop()
  }
}