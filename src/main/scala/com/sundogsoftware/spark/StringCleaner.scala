package com.sundogsoftware.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object StringCleaner {

  def cleanColumn(df: DataFrame, columnName: String, outputCol: String = "cleaned_text"): DataFrame = {
    df.withColumn(
      outputCol,
      trim(
        regexp_replace(
          regexp_replace(col(columnName), "[^A-Za-z0-9 ]+", ""),
          "\\s+", " "
        )
      )
    )
  }
}
