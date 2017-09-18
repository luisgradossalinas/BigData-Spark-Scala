package tablon

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object prueba {
    
  
  def parseoCliDis(line: String) = {
      // Split by commas
      val fields = line.split("\t")
      // Extract the age and numFriends fields, and convert to integers
      val codesta = fields(6).toString
      val monto = fields(12).toString
      val codclavecic = fields(14).toString
      //val ruc = fields(17).toString
      val codPD = fields(47).toString
      //val desPD = fields(48).toString
      // Create a tuple that is our result.
      (codesta, codclavecic)
}
  
  def kpi_establecimiento_x_perfil_digital(ruta: String) = {
    val sc = new SparkContext("local[*]", "prueba")
	val rdd = sc.textFile(ruta)
	val rddPD = rdd.map(parseoCliDis)
	rddPD.countByValue.foreach(println)
}
  
  def main(args: Array[String]) {
    
    /*
    val sc = new SparkContext("local[*]", "prueba")
     val c = sc.parallelize(Array(("est1", 1), ("est1", 1),("est1", 2)))
*/
    
  kpi_establecimiento_x_perfil_digital("tablon.tsv")
    
  }
}