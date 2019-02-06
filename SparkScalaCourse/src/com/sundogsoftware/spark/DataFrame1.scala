package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object DataFrame1 {
  



  
  def main(args: Array[String]) {
    
    
    Logger.getLogger("Org").setLevel(Level.ERROR)
    
    val ssc =  SparkSession
              .builder
              .master("local[*]")
              .appName("DataFrame1")
              .getOrCreate()
              
  val lines = ssc.read.json("..//SparkScalaCourse/SparkScala_SampleData/ml-100k/people.json")
  
  lines.show()
  
  lines.select(lines("name")).show()
  
  lines.groupBy(lines("age")).count().show()
    
  }
  
}