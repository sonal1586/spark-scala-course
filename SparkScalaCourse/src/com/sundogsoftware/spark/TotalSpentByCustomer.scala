package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.tools.nsc.interpreter.Logger

object TotalSpentByCustomer {
  
  def parseLine(lines: String) = {
    val fields = lines.split(",")
    val id = fields(0).toInt
    val amount = fields(2).toFloat
    (id,amount)
    
  }
  
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "TotalSpentByCustomer")
    val rdd = sc.textFile("../SparkScalaCourse/SparkScala_SampleData/ml-100k/customer-orders.csv")
    val lin = rdd.map(parseLine)
    val result = lin.reduceByKey((x,y) => (x+y))
    val flipp = result.map(x => (x._2, x._1))
    val resultsfinal = flipp.sortByKey().collect()
    resultsfinal.foreach(println)
    
    
       
  }
  
}