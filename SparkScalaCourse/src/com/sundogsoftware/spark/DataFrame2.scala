package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import org.apache.log4j._

object DataFrame2 {
  

  
  def main(args: Array[String]) {
    
        
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val ssc =  SparkSession
              .builder
              .master("local[*]")
              .appName("DataFrame2")
              .getOrCreate()
              
  val lines = ssc.sparkContext.textFile("..//SparkScalaCourse/SparkScala_SampleData/ml-100k/people.txt")
  
  import ssc.implicits._
  
  val schemaString = "name age"
  val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType))

  val schema = StructType(fields)
  
  
  val rowRDD = lines.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1).trim))
  
  val peopleDF = ssc.createDataFrame(rowRDD, schema)
  
  peopleDF.show()
  
  peopleDF.select("name").show()
  
  peopleDF.createOrReplaceTempView("people")
  
  val results = ssc.sql("SELECT * FROM people").show()
    
    
  }
  
}