package KPI

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import java.util.ArrayList
import org.apache.spark.rdd.RDD

class Constants {
  
  val sc = new SparkContext("local[*]", "Constants")
  val empty = sc.parallelize(List(("Empty", 0.00)))
  
}