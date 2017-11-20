package hiveTest

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object AllConcepts {

  val sc = new SparkContext("local[*]", "AllConcepts")

  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  val tablonDF = hiveContext.sql("SELECT * FROM LONDON_SMART.TABLON limit 5000000")

  def oneConcept(esta: List[String], inicio: String, fin: String, conceptos: List[String]): DataFrame = {

    val concepto1 = conceptos(0)

    return tablonDF.filter((tablonDF("CODESTABLECIMIENTO") isin (esta: _*)) && (!tablonDF(concepto1).equalTo("null"))
      && (tablonDF("CODMES").between(inicio, fin)))
      .groupBy("CODMES", concepto1)
      .agg(
        countDistinct("CODCLAVECIC_CLIENTE").as("CANT_CLI_DIST"),
        count("CODESTABLECIMIENTO").as("CANT_TOT_TRX"),
        avg("MTOTRANSACCION").as("MONT_PROM_TRX"),
        sum("MTOTRANSACCION").as("MONT_TOT_TRX"),
        (sum("MTOTRANSACCION") / countDistinct("CODCLAVECIC_CLIENTE")).as("MONT_TRX_CLI"),
        (count("RUBRO_BCP") / countDistinct("CODCLAVECIC_CLIENTE")).as("PROM_TRX_CLI")
        )
      .orderBy("CODMES", concepto1)

  }

  def twoConcepts(esta: List[String], inicio: String, fin: String, conceptos: List[String]): DataFrame = {

    val concepto1 = conceptos(0)
    val concepto2 = conceptos(1)

    return tablonDF.filter((tablonDF("CODESTABLECIMIENTO") isin (esta: _*)) && (!tablonDF(concepto1).equalTo("null"))
      && (!tablonDF(concepto2).equalTo("null")) && (tablonDF("CODMES").between(inicio, fin)))
      .groupBy("CODMES", concepto1, concepto2)
      .agg(
        countDistinct("CODCLAVECIC_CLIENTE").as("CANT_CLI_DIST"),
        count("CODESTABLECIMIENTO").as("CANT_TOT_TRX"),
        avg("MTOTRANSACCION").as("MONT_PROM_TRX"),
        sum("MTOTRANSACCION").as("MONT_TOT_TRX"),
        (sum("MTOTRANSACCION") / countDistinct("CODCLAVECIC_CLIENTE")).as("MONT_TRX_CLI"),
        (count("RUBRO_BCP") / countDistinct("CODCLAVECIC_CLIENTE")).as("PROM_TRX_CLI")
      )
      .orderBy("CODMES", concepto1, concepto2)

  }

  def threeConcepts(esta: List[String], inicio: String, fin: String, conceptos: List[String]): DataFrame = {

    val concepto1 = conceptos(0)
    val concepto2 = conceptos(1)
    val concepto3 = conceptos(2)

    return tablonDF.filter((tablonDF("CODESTABLECIMIENTO") isin (esta: _*)) && (!tablonDF(concepto1).equalTo("null"))
      && (!tablonDF(concepto2).equalTo("null")) && (!tablonDF(concepto3).equalTo("null")) && (tablonDF("CODMES").between(inicio, fin)))
      .groupBy("CODMES", concepto1, concepto2, concepto3)
      .agg(
        countDistinct("CODCLAVECIC_CLIENTE").as("CANT_CLI_DIST"),
        count("CODESTABLECIMIENTO").as("CANT_TOT_TRX"),
        avg("MTOTRANSACCION").as("MONT_PROM_TRX"),
        sum("MTOTRANSACCION").as("MONT_TOT_TRX"),
        (sum("MTOTRANSACCION") / countDistinct("CODCLAVECIC_CLIENTE")).as("MONT_TRX_CLI"),
        (count("RUBRO_BCP") / countDistinct("CODCLAVECIC_CLIENTE")).as("PROM_TRX_CLI")
      )
      .orderBy("CODMES", concepto1, concepto2, concepto3)

  }

  def calculoKPI_MiNegocio(esta: List[String], inicio: String, fin: String, conceptos: List[String]): DataFrame = {

    var df = oneConcept(esta, inicio, fin, conceptos)

    if (conceptos.length == 1) {
      df = oneConcept(esta, inicio, fin, conceptos)
    } else if (conceptos.length == 2) {
      df = twoConcepts(esta, inicio, fin, conceptos)
    } else if (conceptos.length == 3) {
      df = threeConcepts(esta, inicio, fin, conceptos)
    }

    return df

  }

  def main(args: Array[String]) {

    AllConcepts.calculoKPI_MiNegocio(List("100070934", "100070905"), "201701", "201702", List("SEXO_CLIENTE", "RANGO_SUELDO", "DESTIPUSODIGITAL")).show()

  }

}