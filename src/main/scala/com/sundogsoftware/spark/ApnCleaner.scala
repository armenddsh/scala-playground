package com.sundogsoftware.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction

object ApnCleaner {

  val cleanApn: UserDefinedFunction = udf((apn: String) => {
    if (apn == null) ""
    else apn.replaceAll("[^A-Za-z0-9]", "")
  })

}
