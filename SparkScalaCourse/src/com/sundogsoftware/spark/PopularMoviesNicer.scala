package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction



object PopularMoviesNicer {
  
  def loadMovieNames() : Map[Int, String] = {
    
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    var movieNames: Map[Int, String] = Map()
    
    var lines = Source.fromFile("../SparkScalaCourse/SparkScala_SampleData/ml-100k/u.item").getLines()
    
    for (line <- lines) {
      var fields = line.split('|')
      
        if (fields.length >1)
        { 
         movieNames += (fields(0).toInt -> fields(1))
        }
    } 
      return movieNames
      
 }
    
    def main(args: Array[String]) {
      
    val sc = new SparkContext("local[*]","PopularMoviesNicer")
    
    val nameDict = sc.broadcast(loadMovieNames)
    
    val lines = sc.textFile("../SparkScalaCourse/SparkScala_SampleData/ml-100k/u.data")
    
    val rdd = lines.map(x => (x.split("\t")(1).toInt,1))
    val rddResults = rdd.reduceByKey((x,y) => (x+y))
    val flipped = rddResults.map(x => (x._2,x._1)).sortByKey()
    val sortedMoviesWithNames = flipped.map(x => (nameDict.value(x._2),x._1))
    val finalresults = sortedMoviesWithNames.collect().foreach(println)
    
     }
  
  }