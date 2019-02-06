package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import scala.io.Source
import scala.math.sqrt


object MovieSimilarities {
  
 
  
def loadMovieNames() : Map[Int, String] = {
  
  
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    
  var movieNames : Map[Int, String] = Map()

  val lines = Source.fromFile("../SparkScalaCourse/SparkScala_SampleData/ml-100k/u.item").getLines()
  
  for (line <- lines) {
    var fields = line.split('|')
    if (fields.length > 1)
    {
      movieNames += (fields(0).toInt -> fields(1))
    }
   } 
  return movieNames
}

  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))

  def filterDuplicates(userRatings:UserRatingPair):Boolean = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2
    
    val movie1 = movieRating1._1
    val movie2 = movieRating2._1
    
    return movie1 < movie2
  }
  
  def makePairs(userRatings: UserRatingPair) = {
    val rdd1 = userRatings._2._1
    val rdd2 = userRatings._2._2
    
    val movie1 = rdd1._1
    val movie2 = rdd2._1
    
    val rating1 = rdd1._2
    val rating2 = rdd2._2
    
    ((movie1,movie2),(rating1, rating2))
    
 }
  
  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]
  
  def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0
    
    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2
      
      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }
    
    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)
    
    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }
    
    return (score, numPairs)
  }
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MovieSimilarities")
    
    val nameDict = loadMovieNames()
    
    val data = sc.textFile("../SparkScalaCourse/SparkScala_SampleData/ml-100k/u.data")
    
    val ratings = data.map(l => l.split("\t")).map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))
    
    //ratings.collect().foreach(println)
    
    val joinedRating = ratings.join(ratings)
    
    //joinedRating.collect().foreach(println)
    
    val uniqueRatings = joinedRating.filter(filterDuplicates)
    
    //uniqueRatings.collect().foreach(println)
    
    val makePairrdd = uniqueRatings.map(makePairs)
    
    //makePairrdd.collect().foreach(println)
    
    val moviePairRatings = makePairrdd.groupByKey()
    
    //moviePairRatings.collect().foreach(println)
    
    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()
    
    //moviePairSimilarities.collect().foreach(println)
        
   
    
    
        
    
    
  }
 
  
}