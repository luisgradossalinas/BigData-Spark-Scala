package Mi_Negocio

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Kpi_Hora {

  val sc = new SparkContext("local[*]", "Kpi_Hora")

  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  val tablonDF = hiveContext.sql("SELECT *,substring(HORTRANSACCION,1,2) as HORA FROM LONDON_SMART.TABLON where (codmes >= 201701 and codmes <= 201707)")

  def KPI_Negocio_Hora(esta: List[String], anio: String): DataFrame = {

    return tablonDF.filter((tablonDF("CODESTABLECIMIENTO") isin (esta: _*)) && tablonDF("CODMES").substr(1, 4).equalTo(anio))
      .groupBy("HORA")
      .agg(
        countDistinct("CODCLAVECIC_CLIENTE").as("CANT_CLI_DIST"),
        count("CODESTABLECIMIENTO").as("CANT_TOT_TRX"),
        avg("MTOTRANSACCION").as("MONT_PROM_TRX"),
        sum("MTOTRANSACCION").as("MONT_TOT_TRX"),
        (sum("MTOTRANSACCION") / countDistinct("CODCLAVECIC_CLIENTE")).as("MONT_TRX_CLI"),
        (count("RUBRO_BCP") / countDistinct("CODCLAVECIC_CLIENTE")).as("PROM_TRX_CLI")
      )
      .orderBy("HORA")

  }

  def main(args: Array[String]) {

    KPI_Negocio_Hora(List("100070934"), "2017")

  }

}