package com.sundogsoftware.spark

object StringCleaner {

  val cleanString: String => String = (input: String) => {
    if (input != null)
      input.trim.replaceAll("[^A-Za-z0-9 ]+", "")
        .replaceAll("\\s+", " ")
        .trim
    else null
  }
}
