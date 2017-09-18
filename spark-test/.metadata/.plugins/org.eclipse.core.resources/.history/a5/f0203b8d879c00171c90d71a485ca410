package tablon

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object countByPerfilDigital {
  


def kpi_establecimiento_x_perfil_digital(ruta: String) = {
   val sc = new SparkContext("local[*]", "countByPerfilDigital")
	val rdd = sc.textFile(ruta)
	val r1 = rdd.map(r => r.split("\t")).map(r => (r(6),r(47)))
	r1.countByValue.foreach(println)
}
  
 def main(args: Array[String]) {
   
      kpi_establecimiento_x_perfil_digital("tablon.tsv")
    
  }
 
  
  
}