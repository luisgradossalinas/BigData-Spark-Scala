package tablon

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import java.util.ArrayList

object clientesRecurrentes {

  /**
   * Data Array del Tablóm
   * Posiciones
   * 17 => RUC
   * 6 => CodEstablecimiento
   * 14 => CodCliente
   * 25 => PeriodoAñoMes
   */
  def kpi_clientes_recurrentes(ruta: String, establecimiento: String, inicio: String) = {

    val sc = new SparkContext("local[*]", "clientesRecurrentes")
    val rangoVisitas = List(3, 6, 12, 24)

    val rddTablon = sc.textFile(ruta)
    val rddDepurado = rddTablon.map(r => r.split("\t")).map(r => (r(17), r(6), r(14), r(25)))

    //1 MES
    val rddFiltrado = rddDepurado.filter(x => (x._4 >= inicio && x._4 <= inicio) && x._2 == establecimiento).map(x => (x._1 + "-" + x._2 + "-" + x._3 + "-" + x._4, 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia1 = rddFiltrado.filter(x => x._2 == 1).map(x => ("1M-01", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia2 = rddFiltrado.filter(x => x._2 == 2).map(x => ("1M-02", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia3_5 = rddFiltrado.filter(x => x._2 >= 3 & x._2 <= 5).map(x => ("1M-03", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia6_10 = rddFiltrado.filter(x => x._2 >= 6 & x._2 <= 10).map(x => ("1M-04", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia11_20 = rddFiltrado.filter(x => x._2 >= 11 & x._2 <= 20).map(x => ("1M-05", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia21_mas = rddFiltrado.filter(x => x._2 >= 21).map(x => ("1M-06", 1)).reduceByKey(_ + _)

    //3 MESES
    val rddFiltrado2 = rddDepurado.filter(x => (x._4 >= "201509" && x._4 <= inicio) && x._2 == establecimiento).map(x => (x._1 + "-" + x._2 + "-" + x._3 + "-" + x._4, 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia2_1 = rddFiltrado2.filter(x => x._2 == 1).map(x => ("3M-01", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia2_2 = rddFiltrado2.filter(x => x._2 == 2).map(x => ("3M-02", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia2_3_5 = rddFiltrado2.filter(x => x._2 >= 3 & x._2 <= 5).map(x => ("3M-03", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia2_6_10 = rddFiltrado2.filter(x => x._2 >= 6 & x._2 <= 10).map(x => ("3M-04", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia2_11_20 = rddFiltrado2.filter(x => x._2 >= 11 & x._2 <= 20).map(x => ("3M-05", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia2_21_mas = rddFiltrado2.filter(x => x._2 >= 21).map(x => ("3M-06", 1)).reduceByKey(_ + _)

    //6 MESES
    val rddFiltrado3 = rddDepurado.filter(x => (x._4 >= "201506" && x._4 <= inicio) && x._2 == establecimiento).map(x => (x._1 + "-" + x._2 + "-" + x._3 + "-" + x._4, 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia3_1 = rddFiltrado3.filter(x => x._2 == 1).map(x => ("6M-01", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia3_2 = rddFiltrado3.filter(x => x._2 == 2).map(x => ("6M-02", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia3_3_5 = rddFiltrado3.filter(x => x._2 >= 3 & x._2 <= 5).map(x => ("6M-03", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia3_6_10 = rddFiltrado3.filter(x => x._2 >= 6 & x._2 <= 10).map(x => ("6M-04", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia3_11_20 = rddFiltrado3.filter(x => x._2 >= 11 & x._2 <= 20).map(x => ("6M-05", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia3_21_mas = rddFiltrado3.filter(x => x._2 >= 21).map(x => ("6M-06", 1)).reduceByKey(_ + _)

    //12 MESES
    val rddFiltrado4 = rddDepurado.filter(x => (x._4 >= "201412" && x._4 <= inicio) && x._2 == establecimiento).map(x => (x._1 + "-" + x._2 + "-" + x._3 + "-" + x._4, 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia4_1 = rddFiltrado4.filter(x => x._2 == 1).map(x => ("12M-01", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia4_2 = rddFiltrado4.filter(x => x._2 == 2).map(x => ("12M-02", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia4_3_5 = rddFiltrado4.filter(x => x._2 >= 3 & x._2 <= 5).map(x => ("12M-03", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia4_6_10 = rddFiltrado4.filter(x => x._2 >= 6 & x._2 <= 10).map(x => ("12M-04", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia4_11_20 = rddFiltrado4.filter(x => x._2 >= 11 & x._2 <= 20).map(x => ("12M-05", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia4_21_mas = rddFiltrado4.filter(x => x._2 >= 21).map(x => ("12M-06", 1)).reduceByKey(_ + _)

    //24 MESES
    val rddFiltrado5 = rddDepurado.filter(x => (x._4 >= "201312" && x._4 <= inicio) && x._2 == establecimiento).map(x => (x._1 + "-" + x._2 + "-" + x._3 + "-" + x._4, 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia5_1 = rddFiltrado5.filter(x => x._2 == 1).map(x => ("24M-01", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia5_2 = rddFiltrado5.filter(x => x._2 == 2).map(x => ("24M-02", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia5_3_5 = rddFiltrado5.filter(x => x._2 >= 3 & x._2 <= 5).map(x => ("24M-03", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia5_6_10 = rddFiltrado5.filter(x => x._2 >= 6 & x._2 <= 10).map(x => ("24M-04", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia5_11_20 = rddFiltrado5.filter(x => x._2 >= 11 & x._2 <= 20).map(x => ("24M-05", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia5_21_mas = rddFiltrado5.filter(x => x._2 >= 21).map(x => ("24M-06", 1)).reduceByKey(_ + _)

    //Unimos todos los RDDs
    val visitasAgrupadas1M = rddVisitas_Frecuencia1.union(rddVisitas_Frecuencia2).union(rddVisitas_Frecuencia3_5)
      .union(rddVisitas_Frecuencia6_10).union(rddVisitas_Frecuencia11_20).union(rddVisitas_Frecuencia21_mas)

    val visitasAgrupadas3M = rddVisitas_Frecuencia2_1.union(rddVisitas_Frecuencia2_2).union(rddVisitas_Frecuencia2_3_5)
      .union(rddVisitas_Frecuencia2_6_10).union(rddVisitas_Frecuencia2_11_20).union(rddVisitas_Frecuencia2_21_mas)

    val visitasAgrupadas6M = rddVisitas_Frecuencia3_1.union(rddVisitas_Frecuencia3_2).union(rddVisitas_Frecuencia3_3_5)
      .union(rddVisitas_Frecuencia3_6_10).union(rddVisitas_Frecuencia3_11_20).union(rddVisitas_Frecuencia3_21_mas)

    val visitasAgrupadas12M = rddVisitas_Frecuencia4_1.union(rddVisitas_Frecuencia4_2).union(rddVisitas_Frecuencia4_3_5)
      .union(rddVisitas_Frecuencia4_6_10).union(rddVisitas_Frecuencia4_11_20).union(rddVisitas_Frecuencia4_21_mas)

    val visitasAgrupadas24M = rddVisitas_Frecuencia5_1.union(rddVisitas_Frecuencia5_2).union(rddVisitas_Frecuencia5_3_5)
      .union(rddVisitas_Frecuencia5_6_10).union(rddVisitas_Frecuencia5_11_20).union(rddVisitas_Frecuencia5_21_mas)

    //val visitasAgrupadas2 = rddVisitas_Frecuencia1_3.join(rddVisitas_Frecuencia2_3)

    /*
    println("aaaaaaaaaaaaaaaa")
    val x = Array(rddVisitas_Frecuencia1.first())
    println(x(1))
    //val x = Array(3,(6))
    
    //val x = Array(3,(1,2,3))
    
    println(x)
    
    */
    //.union(rddVisitas_Frecuencia3_5).union(rddVisitas_Frecuencia6_10).union(rddVisitas_Frecuencia11_20).union(rddVisitas_Frecuencia21_mas)

    //rddVisitas_Frecuencia1.sortByKey(false).foreach(println)

    /*
    println("1 MESES")
    visitasAgrupadas1M.sortByKey().foreach(println)

    println("3 MESES")
    visitasAgrupadas3M.sortByKey().foreach(println)

    println("6 MESES")
    visitasAgrupadas6M.sortByKey().foreach(println)

    println("12 MESES")
    visitasAgrupadas12M.sortByKey().foreach(println)
*/
    //println(x(3))

    //println("Unidos")
    //visitasAgrupadas.union(visitasAgrupadas2).foreach(println)

    val todoJunto = visitasAgrupadas1M.union(visitasAgrupadas3M).union(visitasAgrupadas6M).union(visitasAgrupadas12M).union(visitasAgrupadas24M)
    todoJunto.sortByKey().foreach(println)

  }

  def kpi_clientes_recurrentes_n_establecimientos(ruta: String, est1: String, est2: String, inicio: String, fin: String) = {

    val sc = new SparkContext("local[*]", "clientesRecurrentes")
    val rangoVisitas = List(3, 6, 12, 24)

    val rddTablon = sc.textFile(ruta)
    val rddDepurado = rddTablon.map(r => r.split("\t")).map(r => (r(17), r(6), r(14), r(25)))
    val rddFiltrado = rddDepurado.filter(x => (x._4 >= inicio && x._4 <= fin) && (x._2 == est1 || x._2 == est2)).map(x => (x._1 + "-" + x._2 + "-" + x._3, 1)).reduceByKey(_ + _)

    val rddVisitas_Frecuencia1 = rddFiltrado.filter(x => x._2 == 1).map(x => ("1", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia2 = rddFiltrado.filter(x => x._2 == 2).map(x => ("2", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia3_5 = rddFiltrado.filter(x => x._2 >= 3 & x._2 <= 5).map(x => ("3_5", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia6_10 = rddFiltrado.filter(x => x._2 >= 6 & x._2 <= 10).map(x => ("6_10", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia11_20 = rddFiltrado.filter(x => x._2 >= 11 & x._2 <= 20).map(x => ("11_20", 1)).reduceByKey(_ + _)
    val rddVisitas_Frecuencia21_mas = rddFiltrado.filter(x => x._2 >= 21).map(x => ("21_mas", 1)).reduceByKey(_ + _)


    val visitasAgrupadas = rddVisitas_Frecuencia1.union(rddVisitas_Frecuencia2).union(rddVisitas_Frecuencia3_5)
      .union(rddVisitas_Frecuencia6_10).union(rddVisitas_Frecuencia11_20).union(rddVisitas_Frecuencia21_mas)

    visitasAgrupadas.foreach(println)

  }

  def main(args: Array[String]) {

    println("Establecimiento 100070934")
    kpi_clientes_recurrentes("tablon.tsv", "100070934", "201512")

    //println("Establecimiento 100070905")
    //kpi_clientes_recurrentes("tablon.tsv","100070905","201609","201610")

    //println("Establecimientos 100070934 y 100070905")
    //kpi_clientes_recurrentes_n_establecimientos("tablon.tsv","100070934","100070905","201609","201610")

  }

}