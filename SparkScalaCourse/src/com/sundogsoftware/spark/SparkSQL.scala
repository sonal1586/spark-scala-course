package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._

object SparkSQL {
  
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)
  
  def mapper(line: String): Person ={
    
    val fields = line.split(',')
  
    val person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
    .builder
    .appName("SparkSQL")
    .master("local[*]")
    .config("spark.sql.warehouse.dir","file:///C:/temp")
    .getOrCreate()
    
    val lines = spark.sparkContext.textFile("../SparkScalaCourse/SparkScala_SampleData/ml-100k/fakefriends.csv")
    
    val people = lines.map(mapper)
    
    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    val schemaPeople = people.toDS
    
    schemaPeople.printSchema()
    
    schemaPeople.createOrReplaceTempView("people")
    
    val teenagers = spark.sql("SELECT * from people WHERE age >= 13")
    
    teenagers.collect().foreach(println)
    
    spark.stop()
    
    
    
  }
}