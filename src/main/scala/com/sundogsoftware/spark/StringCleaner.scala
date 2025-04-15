package com.sundogsoftware.spark

object StringCleaner {

  val cleanString: String => String = (input: String) => {
    if (input != null)
      input.replaceAll("\\s+", " ").trim.replaceAll("[^A-Za-z0-9 ]+", "")
    else null
  }
}
