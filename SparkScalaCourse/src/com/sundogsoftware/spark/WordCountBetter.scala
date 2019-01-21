package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object WordCountBetter {
  
  def main(args : Array[String])  {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "WordCount")
    val book = sc.textFile("../SparkScalaCourse/SparkScala_SampleData/ml-100k/book.txt")
    val rdd = book.flatMap(x => x.split(" "))
    val wordCount = rdd.countByValue()
    wordCount.foreach(println)
  }
}