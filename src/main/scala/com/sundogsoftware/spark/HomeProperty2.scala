package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions => F}

object HomeProperty2 {

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
//      .persist(StorageLevel.MEMORY_AND_DISK)

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

    val c1Df = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(s"$homePropertyPath/C1.csv")
    //    .persist(StorageLevel.MEMORY_AND_DISK)

//    val c2Df = spark.read
//      .option("header", value = true)
//      .option("inferSchema", value = true)
//      .csv(s"$homePropertyPath/C2.csv")
//      .persist(StorageLevel.MEMORY_AND_DISK)
//    logger.info("Read REF_202501 successfully")
//
//    val statesDf = spark.read
//      .option("header", value = true)
//      .option("inferSchema", value = true)
//      .csv(s"$homePropertyPath/States.csv")
//      .persist(StorageLevel.MEMORY_AND_DISK)

    logger.info("REF_202501 COUNT: " + dfRef.count())

    logger.info("Reading HS_202501")
    var dfHs = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(s"$homePropertyPath/$hs_filename")
    // .persist(StorageLevel.MEMORY_AND_DISK)

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

    logger.info("Read HS_202501 successfully")

    var dfHomeProperty = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(s"$homePropertyPath/$base_filename")
    //.persist(StorageLevel.MEMORY_AND_DISK)

    dfHomeProperty = dfHomeProperty.select(
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
    // .persist(StorageLevel.MEMORY_AND_DISK)
    dfRef = dfRef.na.fill("")
    //.persist(StorageLevel.MEMORY_AND_DISK)
    dfHs = dfHs.na.fill("")
    // .persist(StorageLevel.MEMORY_AND_DISK)

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

    join_street_type_base_c1 = StringCleaner
      .cleanColumn(join_street_type_base_c1, "full_street_name_base", "full_street_name_base")
    //.persist(StorageLevel.MEMORY_AND_DISK)

    join_street_type_ref_c1 = StringCleaner
      .cleanColumn(join_street_type_ref_c1, "full_street_name_ref", "full_street_name_ref")
    //.persist(StorageLevel.MEMORY_AND_DISK)

    join_street_type_hs_c1 = StringCleaner
      .cleanColumn(join_street_type_hs_c1, "full_street_name_hs", "full_street_name_hs")
    //.persist(StorageLevel.MEMORY_AND_DISK)

    join_street_type_base_c1 = join_street_type_base_c1
      .filter(
        F.col("full_street_name_base").isNotNull && F.trim(F.col("full_street_name_base")) =!= "" &&
          F.col("ZIP5").isNotNull && F.trim(F.col("ZIP5")) =!= ""
      )
    //.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info("dfHomeProperty dropDuplicates")
    join_street_type_base_c1 = join_street_type_base_c1.dropDuplicates("full_street_name_base", "ZIP5")
    //.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info("dfHs filter non-empty")
    join_street_type_hs_c1 = join_street_type_hs_c1
      .filter(
        F.col("full_street_name_hs").isNotNull && F.trim(F.col("full_street_name_hs")) =!= "" &&
          F.col("zip").isNotNull && F.trim(F.col("zip")) =!= ""
      )
    //.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info("dfHs dropDuplicates")
    join_street_type_hs_c1 = join_street_type_hs_c1.dropDuplicates("full_street_name_hs", "zip")
    //.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info("dfRef filter non-empty")
    join_street_type_ref_c1 = join_street_type_ref_c1
      .filter(
        F.col("full_street_name_ref").isNotNull && F.trim(F.col("full_street_name_ref")) =!= "" &&
          F.col("zip").isNotNull && F.trim(F.col("zip")) =!= ""
      )
    //.persist(StorageLevel.MEMORY_AND_DISK)


    logger.info("dfRef dropDuplicates")
    join_street_type_ref_c1 = join_street_type_ref_c1.dropDuplicates("full_street_name_ref", "zip")
    //.persist(StorageLevel.MEMORY_AND_DISK)

    val df_ref_columns = prefix_columns("ref", "ref", (join_street_type_ref_c1.columns))
    val df_base_columns = prefix_columns("base", "base", (join_street_type_base_c1.columns))
    val df_hs_columns = prefix_columns("hs", "hs", (join_street_type_hs_c1.columns))

    logger.info("join dfHomeProperty - dfHs")


    join_street_type_base_c1.show(truncate = false, numRows = 1000)
    join_street_type_hs_c1.show(truncate = false, numRows = 1000)
    join_street_type_ref_c1.show(truncate = false, numRows = 1000)

    val joined_dfHomeProperty_dfHs = join_street_type_base_c1.alias("base")
      .join(join_street_type_hs_c1.alias("hs"),
        F.col("base.ZIP5") === F.col("hs.zip") &&
          F.col("base.full_street_name_base") === F.col("hs.full_street_name_hs")
      )
      .select(df_hs_columns ++ df_base_columns: _*)
    //.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info("join dfHomeProperty - dfRef")

    val joined_dfHomeProperty_dfRef = join_street_type_base_c1.alias("base")
      .join(join_street_type_ref_c1.alias("ref"),
        F.col("base.ZIP5") === F.col("ref.zip") &&
          F.col("base.full_street_name_base") === F.col("ref.full_street_name_ref")
      )
      .select(df_ref_columns ++ df_base_columns: _*)
    //.persist(StorageLevel.MEMORY_AND_DISK)


    val joined_dfHomeProperty_dfHs_not_matched = join_street_type_hs_c1.alias("hs")
      .join(join_street_type_base_c1.alias("base"),
        F.col("base.ZIP5") === F.col("hs.zip") &&
          F.col("base.full_street_name_base") === F.col("hs.full_street_name_hs"),
        "leftanti"
      )
    //.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info("join dfHomeProperty - dfRef")

    val joined_dfHomeProperty_dfRef_not_matched = join_street_type_ref_c1.alias("ref")
      .join(join_street_type_base_c1.alias("base"),
        F.col("base.ZIP5") === F.col("ref.zip") &&
          F.col("base.full_street_name_base") === F.col("ref.full_street_name_ref"),
        "leftanti"
      )

    logger.info("Saving Home Sales - Home Property joined DataFrame to CSV")
    joined_dfHomeProperty_dfHs
      .na.fill("")
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$homePropertyPath/df_join_hs_home_property_3_matched")

    logger.info("Saving Refinance - Home Property joined DataFrame to CSV")
    joined_dfHomeProperty_dfRef
      .na.fill("")
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$homePropertyPath/df_join_ref_home_property_3_matched")

    logger.info("Saving Home Sales - Home Property joined DataFrame Not matched to CSV")

    joined_dfHomeProperty_dfHs_not_matched
      .na.fill("")
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$homePropertyPath/df_join_hs_home_property_3_not_matched")

    logger.info("Saving Ref - Home Property joined Not matched DataFrame to CSV")

    joined_dfHomeProperty_dfRef_not_matched
      .na.fill("")
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$homePropertyPath/df_join_ref_home_property_3_not_matched")

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