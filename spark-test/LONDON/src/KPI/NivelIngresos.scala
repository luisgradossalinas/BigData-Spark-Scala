package KPI

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import java.util.ArrayList
import org.apache.spark.rdd.RDD

object NivelIngresos {

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

  /**
   * Función que devuelve que no hay datos
   */
  def dataEmpty(): RDD[(String, Int)] = {

    val sc = new SparkContext("local[*]", "NivelIngresos")
    val c = sc.parallelize(List(("Empty", 0)))
    return c
  }

  def byClientes(ruta: String, esta: List[String], inicio: String, fin: String): RDD[(String, Int)] = {

    val sc = new SparkContext("local[*]", "NivelIngresos")
    val rdd = sc.textFile(ruta)
    val r1 = rdd.map(r => r.split("\t")).map(r => (r(6), r(25), r(46)))

    var rddFiltrado = r1.filter(x => (x._2 >= inicio && x._2 <= fin) && esta.exists(p => p.contains(x._1) && x._3 != "\\N"))
    val res = rddFiltrado.map(x => (x._3, 1)).reduceByKey(_ + _).sortByKey()

    if (res.count() < 1) {
      return sc.parallelize(List(("Empty", 0)))
    } else {
      return res
    }

  }

  /**
   * Devuelve la suma de montos de las transacciones que existen por cada Nivel de Ingresos por meses
   */
  def evolucionCompras(ruta: String, esta: List[String], inicio: String, fin: String): RDD[(String, Float)] = {

    val sc = new SparkContext("local[*]", "NivelIngresos")
    val rdd = sc.textFile(ruta)
    val r1 = rdd.map(r => r.split("\t")).map(r => (r(6), r(25), r(46), r(12).toFloat))

    var rddFiltrado = r1.filter(x => (x._2 >= inicio && x._2 <= fin) && esta.exists(p => p.contains(x._1) && x._3 != "\\N"))
    val res = rddFiltrado.map(x => (x._2 + "-" + x._3, x._4.toFloat)).reduceByKey(_ + _).sortByKey()

    if (res.count() < 1) {
      return sc.parallelize(List(("Empty", 0)))
    } else {
      return res
    }

  }

  /**
   * Devuelve el promedio de compra según Nivel de Ingresos por un rango de fechas especificado
   */
  def montoPromedio(ruta: String, esta: List[String], inicio: String, fin: String): RDD[(String, Float)] = {

    val sc = new SparkContext("local[*]", "NivelIngresos")
    val rdd = sc.textFile(ruta)
    val r1 = rdd.map(r => r.split("\t")).map(r => (r(6), r(25), r(46), r(12).toDouble))

    var rddFiltrado = r1.filter(x => (x._2 >= inicio && x._2 <= fin) && esta.exists(p => p.contains(x._1) && x._3 != "\\N")).map(r => (r._3, r._4))
    val res = rddFiltrado.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => (x._1.toFloat / x._2.toFloat).toFloat)

    if (res.count() < 1) {
      return sc.parallelize(List(("Empty", 0)))
    } else {
      return res
    }

  }

  def main(args: Array[String]) {

    //val x = byClientes("tablon.tsv", List("100070934", "100070905"), "201501", "201512")
    //x.foreach(println)

    val y = evolucionCompras("tablon.tsv", List("100070934", "100070905"), "201501", "201512")
    y.foreach(println)

    //val z = montoPromedio("tablon.tsv", List("508843301"), "201501", "201512") //508843301
    //z.foreach(println)

  }

}