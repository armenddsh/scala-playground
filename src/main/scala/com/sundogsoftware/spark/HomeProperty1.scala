package com.sundogsoftware.spark
import com.sundogsoftware.spark.AddressStandardizer._
import org.apache.log4j._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions => F}

object HomeProperty1 {

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

    val homePropertyPath = "data/homeproperty"

    logger.info("Reading input CSV files")
    var dfRecorder = spark.read.option("header", "true").option("inferSchema", "true").csv(s"$homePropertyPath/Recorder_202501.csv")
    var dfRef = spark.read.option("header", "true").option("inferSchema", "true").csv(s"$homePropertyPath/REF_202501.csv")
    var dfHs = spark.read.option("header", "true").option("inferSchema", "true").csv(s"$homePropertyPath/HS_202501.csv")

    logger.info("Adding unique IDs to DataFrames")
    val window = Window.orderBy(F.monotonically_increasing_id())
    dfRecorder = dfRecorder.withColumn("_id", F.row_number().over(window))
    dfRef = dfRef.withColumn("_id", F.row_number().over(window))
    dfHs = dfHs.withColumn("_id", F.row_number().over(window))

    logger.info("Filling null values in DataFrames")
    dfRecorder = dfRecorder.na.fill("")
    dfRef = dfRef.na.fill("")
    dfHs = dfHs.na.fill("")

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

    dfRecorder = standardizeAddress(dfRecorder, "PropertyAddressStreetName", "PropertyAddressStreetName")
    dfHs = standardizeAddress(dfHs, "full_street_name_hs", "full_street_name_hs")
    dfRef = standardizeAddress(dfRef, "full_street_name_ref", "full_street_name_ref")

    dfRecorder = dfRecorder.dropDuplicates("PropertyAddressFull", "PropertyAddressZIP")
    dfHs = dfHs.dropDuplicates("full_street_name_hs", "zip")
    dfRef = dfRef.dropDuplicates("full_street_name_ref", "zip")

    val joined_dfRecorder_dfHs = dfRecorder.alias("p")
      .join(dfHs.alias("h"),
        F.col("p.PropertyAddressZIP") === F.col("h.zip") &&
          F.col("p.PropertyAddressFull") === F.col("h.full_street_name_hs")
      )

    // Select distinct records with formatted addresses
    val resultDF_dfRecorder_dfHs = joined_dfRecorder_dfHs.select(
      F.col("p.APNFormatted"),
      F.concat_ws(" ",
        F.col("p.PropertyAddressFull"),
        F.col("p.PropertyAddressCity"),
        F.col("p.PropertyAddressState"),
        F.col("p.PropertyAddressZIP")
      ).alias("RecorderAddressProperty"),
      F.concat_ws(" ",
        F.col("h.street_number"),
        F.col("h.direction_one"),
        F.col("h.street"),
        F.col("h.street_type"),
        F.col("h.direction_two"),
        F.col("h.suite"),
        F.col("h.city"),
        F.col("h.state"),
        F.col("h.zip")
      ).alias("HomeSalesAddress")
    ).distinct()

    val joined_dfRecorder_dfRef = dfRecorder.alias("p")
      .join(dfRef.alias("h"),
        F.col("p.PropertyAddressZIP") === F.col("h.zip") &&
          F.col("p.PropertyAddressFull") === F.col("h.full_street_name_ref")
      )

    val result_dfRecorder_dfRef = joined_dfRecorder_dfRef.select(
      F.col("p.APNFormatted"),
      F.concat_ws(" ",
        F.col("p.PropertyAddressFull"),
        F.col("p.PropertyAddressCity"),
        F.col("p.PropertyAddressState"),
        F.col("p.PropertyAddressZIP")
      ).alias("RecorderAddressProperty"),
      F.concat_ws(" ",
        F.col("h.street_number"),
        F.col("h.direction_one"),
        F.col("h.street"),
        F.col("h.street_type"),
        F.col("h.direction_two"),
        F.col("h.suite"),
        F.col("h.city"),
        F.col("h.state"),
        F.col("h.zip")
      ).alias("RefiAddress")
    ).distinct()

    logger.info(result_dfRecorder_dfRef.count())
    logger.info(resultDF_dfRecorder_dfHs.count())

    logger.info("Saving Home Sales - Recorder joined DataFrame to CSV")
    resultDF_dfRecorder_dfHs
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$homePropertyPath/df_join_hs_recorder")

    logger.info("Saving Refinance - Recorder joined DataFrame to CSV")
    result_dfRecorder_dfRef
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$homePropertyPath/df_join_ref_rec")

    logger.info("Stopping Spark session")
    spark.stop()
  }
}