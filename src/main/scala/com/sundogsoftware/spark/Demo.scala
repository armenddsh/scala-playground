package com.sundogsoftware.spark

import org.apache.spark
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._


object Demo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("LandUseCodes")
      .master("local[*]") // use your cluster master in production
      .getOrCreate()

    // Define the schema
    val schema = StructType(List(
      StructField("street", StringType, nullable = true)
    ))

    // Example data rows — add more as needed
    val data = Seq(
      Row("Test # road"),
      // Add more rows...
    )

    // Create DataFrame
    var df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df = AddressStandardizer.standardizeAddress(df, "street", "street_standardized")
    df.show()

  }
}
