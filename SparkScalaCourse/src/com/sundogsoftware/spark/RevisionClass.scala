package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.io.Source
import org.apache.spark.sql.functions._


object RevisionClass {
  
  def popularMovies() : Map[Int,String] = {
    
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    var movies:Map[Int,String] = Map()
    
    val lines = Source.fromFile("../SparkScalaCourse/SparkScala_SampleData/ml-100k/u.item").getLines()
    
    for(line <- lines)
    {
      val fields = line.split('|')
      if (fields.length >1) {
        movies += (fields(0).toInt -> fields(1))
      
    }
    }
    return  movies
    
  }
  
  final case class Movie(ID: Int)
  
  def main(args: Array[String])
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val ssc =  SparkSession
              .builder()
              .master("local[*]")
              .appName("RevisionClass")
              //.config("spark.sql.warehouse.dir", "file:///C:/temp")
              .getOrCreate()
              
     val lines = ssc.sparkContext.textFile("../SparkScalaCourse/SparkScala_SampleData/ml-100k/u.data").map(x => Movie(x.split("\t")(1).toInt))
     
     val moviesDS = popularMovies()
     
     import ssc.implicits._
     val rddd = lines.toDS()
     
     val results = rddd.groupBy("ID").count().sort("count").take(20)
     

     
     //val results = rddd.groupBy("ID").count().orderBy(desc("count")).cache()
     
     results.foreach(println)
     
     
    for (result <- results) {
      // result is just a Row at this point; we need to cast it back.
      // Each row has movieID, count as above.
      println (moviesDS(result(0).asInstanceOf[Int]) + ": " + result(1))
    }
     
    ssc.stop()
     
     
     
    
    
  }
}