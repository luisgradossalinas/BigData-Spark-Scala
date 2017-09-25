package KPI

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import java.util.ArrayList
import org.apache.spark.rdd.RDD

object Edad {
  /**
   * Data del Tablón
   * 17 => RUC
   * 6 => CODESTABLECIMIENTO
   * 12 => MTOTRANSACCION
   * 14 => CODCLAVECIC_CLIENTE
   * 25 => CODMES
   * 43 => SEXO_CLIENTE
   * 46 => RANGO_SUELDO
   * 47 => TIPUSODIGITAL
   * 48 => DESTIPUSODIGITAL
   * 57 => RANGO_EDAD
   */

  def byClientes(ruta: String, esta: List[String], inicio: String, fin: String): RDD[(String, Int)] = {

    val sc = new SparkContext("local[*]", "Edad")
    val rdd = sc.textFile(ruta)
    val r1 = rdd.map(r => r.split("\t")).map(r => (r(6), r(25), r(57)))

    var rddFiltrado = r1.filter(x => (x._2 >= inicio && x._2 <= fin) && esta.exists(p => p.contains(x._1) && x._3 != "\\N"))
    val res = rddFiltrado.map(x => (x._3, 1)).reduceByKey(_ + _).sortByKey()
    return res

  }

  def evolucionCompras(ruta: String, esta: List[String], inicio: String, fin: String): RDD[(String, Double)] = {

    val sc = new SparkContext("local[*]", "Edad")
    val rdd = sc.textFile(ruta)
    val r1 = rdd.map(r => r.split("\t")).map(r => (r(6), r(25), r(57), r(12).toDouble))

    var rddFiltrado = r1.filter(x => (x._2 >= inicio && x._2 <= fin) && esta.exists(p => p.contains(x._1) && x._3 != "\\N"))
    val res = rddFiltrado.map(x => (x._2 + "-" + x._3, x._4.toDouble)).reduceByKey(_ + _).sortByKey()
    return res

  }

  def montoPromedio(ruta: String, esta: List[String], inicio: String, fin: String): RDD[(String, Double)] = {

    val sc = new SparkContext("local[*]", "Edad")
    val rdd = sc.textFile(ruta)
    val r1 = rdd.map(r => r.split("\t")).map(r => (r(6), r(25), r(57), r(12).toDouble))

    var rddFiltrado = r1.filter(x => (x._2 >= inicio && x._2 <= fin) && esta.exists(p => p.contains(x._1) && x._3 != "\\N")).map(r => (r._3, r._4))

    rddFiltrado.foreach(println)

    val res = rddFiltrado.map(x => (x._1, x._2.toDouble)).reduceByKey(_ + _).sortByKey()

    return res

    //val x = rddFiltrado.mapValues((_, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues{case (sum, count) => (1.0 * sum)/count}.collectAsMap()
    //val x = rddFiltrado.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues{ case (sum, count) => (1.0 * sum)/count}.collectAsMap()

    //x.foreach(println)

    //mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(y => 1.0 * y._1 / y._2).collect

  }

  def main(args: Array[String]) {

    //val x = byClientes("tablon.tsv", List("100070934", "100070905"), "201501", "201512")
    //x.foreach(println)

    //val y = evolucionCompras("tablon.tsv", List("100070934", "100070905"), "201501", "201512")
    //y.foreach(println)

    val z = montoPromedio("tablon.tsv", List("100070905"), "201512", "201512")
    z.foreach(println)

  }

}