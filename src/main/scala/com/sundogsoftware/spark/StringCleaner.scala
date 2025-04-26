package com.sundogsoftware.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object StringCleaner {

  def cleanColumn(df: DataFrame,colName: String, outputColName: String): DataFrame = {
    df.withColumn(
      outputColName,
      trim(
        regexp_replace(
          regexp_replace(col(colName), "[^A-Za-z0-9 ]+", ""),
          "\\s+", " "
        )
      )
    )
  }
}
