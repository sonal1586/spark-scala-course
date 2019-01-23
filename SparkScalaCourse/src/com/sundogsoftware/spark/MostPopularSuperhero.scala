package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import shapeless.ops.nat.ToInt


object MostPopularSuperhero {
  
  def countToOccurance(line: String) = {
    
    var fields = line.split("\\s+")
    (fields(0).toInt, fields.length -1)
  }
  
  def parseNames(line : String): Option[(Int,String)] = {
    
    var fields = line.split('\"')
    if (fields.length > 1)
    return Some(fields(0).trim().toInt, fields(1))
    
    else 
      return None
    }
  
  
  def main(args: Array[String]) {
    
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","MostPopularSuperhero")
  val names = sc.textFile("../SparkScalaCourse/SparkScala_SampleData/ml-100k/Marvel-names.txt")
  val line = sc.textFile("../SparkScalaCourse/SparkScala_SampleData/ml-100k/Marvel-graph.txt")
  
  val rdd1 = names.flatMap(parseNames)
  
  val rdd2 = line.map(countToOccurance)
  
  val popularhero = rdd2.reduceByKey((x,y) => (x+y))
  
  val flipped = popularhero.map(x => (x._2, x._1))
  
  val mostPopular = flipped.max()
  
  val mostPopularName = rdd1.lookup(mostPopular._2)(0)
    println(mostPopularName)
    // Print out our answer!
    println(s"$mostPopularName is the most popular superhero with ${mostPopular._1} co-appearances.") 
  
  }
  
}