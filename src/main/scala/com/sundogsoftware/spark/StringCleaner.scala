package com.sundogsoftware.spark

import scala.util.matching.Regex

object StringCleaner {

  private val patternsClean1: (Regex, String) = "[^A-Za-z0-9 ]+".r -> ""
  private val patternsClean2: (Regex, String) = "\\s+".r -> " "

  val cleanString: String => String = (input: String) => {
    if (input != null) {
      val cleanedOnce = patternsClean1._1.replaceAllIn(input, patternsClean1._2)
      patternsClean2._1.replaceAllIn(cleanedOnce, patternsClean2._2).trim
    } else null
  }
}
