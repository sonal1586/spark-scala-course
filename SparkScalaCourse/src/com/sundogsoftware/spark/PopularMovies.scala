package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object PopularMovies {
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]","PopularMovies")
    val lines = sc.textFile("../SparkScalaCourse/SparkScala_SampleData/ml-100k/u.data")
    
    //***   Two Meathods same problem   ***//
    
    val rdd = lines.map(x => (x.split("\t")(1).toInt,1))
    val rddResults = rdd.reduceByKey((x,y) => (x+y))
    val flipped = rddResults.map(x => (x._2,x._1)).sortByKey().collect()
    flipped.foreach(println)
    
    
    val rdd2 = lines.map(x => (x.split("\t")(1).toInt))
    val rddResults2 = rdd2.countByValue()
    val flipped2 = rddResults2.map(x => (x._2,x._1))
    val rddSeq = flipped2.toSeq.sortBy(x => x._1)
    
    rddSeq.foreach(println)
    
    
    
    
    
    
    
  }
  
}