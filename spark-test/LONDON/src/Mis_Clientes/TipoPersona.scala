package Mis_Clientes

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object TipoPersona {

  val sc = new SparkContext("local[*]", "TipoPersona")

  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  val tablonDF = hiveContext.sql("SELECT * FROM LONDON_SMART.TABLON where (codmes >= 201701 and codmes <= 201707)")

  def KPI_MisClientes_TipoPersona(esta: List[String], rubro: String, anio: String): DataFrame = {

    return tablonDF.filter((tablonDF("CODESTABLECIMIENTO") isin (esta: _*))
      && (!tablonDF("TIPO_PERSONA").equalTo("null")) && (tablonDF("CODMES").substr(1, 4).equalTo(anio)))
      .groupBy("CODMES", "TIPO_PERSONA")
      .agg(
        countDistinct("CODCLAVECIC_CLIENTE").as("CANT_CLI_DIST"),
        count("CODESTABLECIMIENTO").as("CANT_TOT_TRX"),
        avg("MTOTRANSACCION").as("MONT_PROM_TRX"),
        sum("MTOTRANSACCION").as("MONT_TOT_TRX"),
        (sum("MTOTRANSACCION") / countDistinct("CODCLAVECIC_CLIENTE")).as("MONT_TRX_CLI"),
        (count("RUBRO_BCP") / countDistinct("CODCLAVECIC_CLIENTE")).as("PROM_TRX_CLI"))
      .orderBy("CODMES", "TIPO_PERSONA")

  }

  def main(args: Array[String]) {

    KPI_MisClientes_TipoPersona(List("100002009"), "SUPERMERCADOS Y MINIMERCADOS", "2017")
    
    //100002009-RECREACION-TIPO_DE_PERSONA-MES-ANUAL-2017
    
  }

}