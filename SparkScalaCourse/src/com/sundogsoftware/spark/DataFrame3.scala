package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

import org.apache.spark.sql.types.{StructField, StringType, StructType}



object DataFrame3 {
  
  case class Person(name:String, age:Int)
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
                .builder()
                .appName("DataFrame3")
                .master("local[*]")
                .getOrCreate()
                
                
    val lines = spark.sparkContext.textFile("../SparkScalaCourse/SparkScala_SampleData/ml-100k/u.data")
    
    import spark.implicits._
    
    val rddLines = lines.toDF("line")
    
    rddLines.select(rddLines("line")).show()
    rddLines.filter(rddLines("line").like("196%")).show()
    
    
    /*******      Creating DataFrame using Case Classes in Scala      *********/
    
    
    
    val people = Seq(Person("Rohit",32),Person("Ajay",28))
    
    val rddDf = spark.createDataFrame(people)
    
   rddDf.show()
   
   /********     Custom DataFrame Creation using createDataFrame      ******/
   
   /**   https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-DataFrame.html   **/
   
   val winner = spark.sparkContext.textFile("../SparkScalaCourse/SparkScala_SampleData/ml-100k/Winners_Curse.csv")
   
   val header = winner.first()
   
   val rddSchema = header.split(",").map(x => x.trim()).map(f => StructField(f, StringType))
   
   val schema = StructType(rddSchema)
   
   val nonHeader = winner.filter(x => x!=header).map(x => x.split(",")).map(f => Row.fromSeq(f))
   
   
   
   val rddDataFrame = spark.createDataFrame(nonHeader, schema)
   
   rddDataFrame.show()
   
   
   /*********   Creating DataFrame from CSV files using spark-csv module       **********/
   
   val df = spark.read.format("csv").load("../SparkScalaCourse/SparkScala_SampleData/ml-100k/Winners_Curse.csv")
   
   df.printSchema()
  
    /*********   Working With Parquet File Format       **********/
   
   
  
    
  }
  
}