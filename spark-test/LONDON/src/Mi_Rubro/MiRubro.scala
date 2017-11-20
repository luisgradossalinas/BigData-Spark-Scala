package Mi_Rubro

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object MiRubro {
  
  val sc = new SparkContext("local[*]", "MiRubro")

  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  val tablonDF = hiveContext.sql("SELECT * FROM LONDON_SMART.TABLON where (codmes >= 201701 and codmes <= 201702) and rubro_bcp is not null")

  def KPI_RubroGeneralMesAnual(rubro: String, fecha: String): DataFrame = {

    return tablonDF.filter((tablonDF("RUBRO_BCP").equalTo(rubro)) && (tablonDF("CODMES").substr(1, 4).equalTo(fecha)))
      .groupBy("CODMES")
      .agg(
        countDistinct("CODCLAVECIC_CLIENTE").as("CANT_CLI_DIST"),
        count("CODESTABLECIMIENTO").as("CANT_TOT_TRX"),
        avg("MTOTRANSACCION").as("MONT_PROM_TRX"),
        sum("MTOTRANSACCION").as("MONT_TOT_TRX"),
        (sum("MTOTRANSACCION") / countDistinct("CODCLAVECIC_CLIENTE")).as("MONT_TRX_CLI"),
        (count("RUBRO_BCP") / countDistinct("CODCLAVECIC_CLIENTE")).as("PROM_TRX_CLI"))
      .orderBy("CODMES")

  }

  def KPI_RubroProcedenciaMesAnual(rubro: String, fecha: String): DataFrame = {

    return tablonDF.filter((tablonDF("RUBRO_BCP").equalTo(rubro)) && (!tablonDF("CODCLAVECIC_CLIENTE").equalTo("null"))
      && (!tablonDF("DEPARTAMENTO_ESTABLEC").equalTo("null")))
      .groupBy("CODMES", "DEPARTAMENTO_ESTABLEC")
      .agg(
        countDistinct("CODCLAVECIC_CLIENTE").as("CANT_CLI_DIST"),
        count("CODESTABLECIMIENTO").as("CANT_TOT_TRX"),
        avg("MTOTRANSACCION").as("MONT_PROM_TRX"),
        sum("MTOTRANSACCION").as("MONT_TOT_TRX"),
        (sum("MTOTRANSACCION") / countDistinct("CODCLAVECIC_CLIENTE")).as("MONT_TRX_CLI"),
        (count("RUBRO_BCP") / countDistinct("CODCLAVECIC_CLIENTE")).as("PROM_TRX_CLI"))
      .orderBy("CODMES", "DEPARTAMENTO_ESTABLEC")

  }

  def KPI_RubroGeneralDiaMes(rubro: String, fecha: String): DataFrame = {

    return tablonDF.filter((tablonDF("RUBRO_BCP").equalTo(rubro))
      && (tablonDF("CODMES").equalTo(fecha)))
      .groupBy("FECEFECTIVA")
      .agg(
        countDistinct("CODCLAVECIC_CLIENTE").as("CANT_CLI_DIST"),
        count("CODESTABLECIMIENTO").as("CANT_TOT_TRX"),
        avg("MTOTRANSACCION").as("MONT_PROM_TRX"),
        sum("MTOTRANSACCION").as("MONT_TOT_TRX"),
        (sum("MTOTRANSACCION") / countDistinct("CODCLAVECIC_CLIENTE")).as("MONT_TRX_CLI"),
        (count("RUBRO_BCP") / countDistinct("CODCLAVECIC_CLIENTE")).as("PROM_TRX_CLI"))
      .orderBy("FECEFECTIVA")

  }

  def main(args: Array[String]) {

    KPI_RubroGeneralMesAnual("RESTAURANTES", "2017").show()
    KPI_RubroGeneralDiaMes("RESTAURANTES", "201701").show()
    KPI_RubroProcedenciaMesAnual("RESTAURANTES", "2017").show()

  }

}