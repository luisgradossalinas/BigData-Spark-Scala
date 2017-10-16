package KPI

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import shapeless._0

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

    val sc = new SparkContext("local[*]", "prueba")
    val rdd = sc.parallelize(Array(("BAJO", 1), ("MEDIO", 5), ("ALTO", 2)))

    val tipo = sc.parallelize(Array(("BAJO"), ("MEDIO"), ("ALTO"), ("MUY ALTO")))

    var data = new Array[(String, Int)](tipo.count().toInt)
    var contador: Int = 0

    var countRDD = rdd.count().toInt

    for (name <- rdd.collect()) {
      data(contador) = (name._1, name._2)
      contador = contador + 1
    }

    for (name <- data) {
      for (t <- tipo.collect()) {


        /*
        if (t != name._1){
          //data(countRDD-1) = (t, 0)
          //countRDD = countRDD + 1
          println(t)
        }*/
        
        println(t +"-"+name._1) 
        

      }

    }

    //data(3) = ("MUY ALTO", 0)

    //println("prueba 1")
    //println(data.mkString(","))
    //println(data.mkString("|"))

    //kpi_establecimiento_x_perfil_digital("tablon.tsv")

  }
}