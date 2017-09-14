package tablon

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object ventasxEstablecimiento {
  

def kpi_ventas_x_establecimientos(ruta: String) = {
   val sc = new SparkContext("local[*]", "ventasxEstablecimiento")
	val rdd = sc.textFile(ruta)
	val r1 = rdd.map(r => r.split("\t")).map(r => (r(6),r(12).toDouble))
	r1.reduceByKey(_+_).foreach(println)
}

 def main(args: Array[String]) {
   
      kpi_ventas_x_establecimientos("tablon.tsv")
    
  }

  
}