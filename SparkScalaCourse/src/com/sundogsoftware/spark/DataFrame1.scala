package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.sql.types.{StructType,StructField}
import org.apache.spark.SparkContext._

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType

object DataFrame1 {
  



  
  def main(args: Array[String]) {
    
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val ssc =  SparkSession
              .builder
              .master("local[*]")
              .appName("DataFrame1")
              .getOrCreate()
              
  val lines = ssc.read.json("..//SparkScalaCourse/SparkScala_SampleData/ml-100k/people.json")
  
  lines.printSchema()
  
  lines.show()
  
  lines.select(lines("name")).show()
  
  lines.groupBy(lines("age")).count().show()
  
  
  
  
val schemaUntyped = StructType(
  List(
    StructField("age", LongType, true),
    StructField("name", StringType, true)
  )
)
  
 
  
  val schemaRdd = ssc.read.schema(schemaUntyped).json("..//SparkScalaCourse/SparkScala_SampleData/ml-100k/people.json")
  

  schemaRdd.printSchema()
  
  schemaRdd.select("age", "name").show()
    
  }
  
}