package com.sundogsoftware.spark

import org.apache.spark.sql.{functions => F}

object Utils {
  def prefix_columns(prefix: String, long_prefix: String, columns: Array[String]): Array[org.apache.spark.sql.Column] = {
    columns.map(col => F.col(s"$prefix.$col").alias(s"${col}_$long_prefix"))
  }
}
