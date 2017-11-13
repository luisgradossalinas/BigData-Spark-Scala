package TestDataFrame

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SexoFrame {
  /*
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

  var spark = SparkSession
    .builder
    .appName("PocDF")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .getOrCreate()

  def byClientes(ruta: String, esta: List[String], inicio: String, fin: String): DataFrame = {

    val rddTablon = spark.sparkContext.textFile(ruta).map(r => r.split("\t")).map(r => Row(r(6), r(25), r(43)))
    val cabeceras = "CODESTABLECIMIENTO CODMES SEXO_CLIENTE"
    val camposDF = cabeceras.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(camposDF)

    val tablonDF = spark.createDataFrame(rddTablon, schema)
    return tablonDF.filter((tablonDF("CODESTABLECIMIENTO") isin (esta: _*)) && (!tablonDF("SEXO_CLIENTE").equalTo("\\N")) && (tablonDF("CODMES").between(inicio, fin))).groupBy("SEXO_CLIENTE").count()

  }

  def evolucionCompras(ruta: String, esta: List[String], inicio: String, fin: String): DataFrame = {

    val rddTablon = spark.sparkContext.textFile(ruta).map(r => r.split("\t")).map(r => Row(r(6), r(25), r(43), r(12).toDouble))

    val schema = new StructType()
      .add("CODESTABLECIMIENTO", StringType, true)
      .add("CODMES", StringType, true)
      .add("SEXO_CLIENTE", StringType, true)
      .add("MTOTRANSACCION", DoubleType, true)

    val tablonDF = spark.createDataFrame(rddTablon, schema)
    return tablonDF.filter((tablonDF("CODESTABLECIMIENTO") isin (esta: _*)) && (!tablonDF("SEXO_CLIENTE").equalTo("\\N")) && (tablonDF("CODMES").between(inicio, fin)))
      .groupBy("CODMES", "SEXO_CLIENTE").sum("MTOTRANSACCION").as("Total").orderBy("CODMES", "SEXO_CLIENTE")

  }
  
  def montoPromedio(ruta: String, esta: List[String], inicio: String, fin: String): DataFrame = {

    val rddTablon = spark.sparkContext.textFile(ruta).map(r => r.split("\t")).map(r => Row(r(6), r(25), r(43), r(12).toDouble))

    val schema = new StructType()
      .add("CODESTABLECIMIENTO", StringType, true)
      .add("CODMES", StringType, true)
      .add("SEXO_CLIENTE", StringType, true)
      .add("MTOTRANSACCION", DoubleType, true)

    val tablonDF = spark.createDataFrame(rddTablon, schema)
    return tablonDF.filter((tablonDF("CODESTABLECIMIENTO") isin (esta: _*)) && (!tablonDF("SEXO_CLIENTE").equalTo("\\N")) && (tablonDF("CODMES").between(inicio, fin)))
      .groupBy("CODMES","SEXO_CLIENTE").avg("MTOTRANSACCION").orderBy("CODMES", "SEXO_CLIENTE")

  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val x = byClientes("tablon.tsv", List("100070934", "100070905"), "201501", "201512")
    x.show()

    val y = evolucionCompras("tablon.tsv", List("100070934", "100070905"), "201501", "201512")
    y.show()
    
    val z = montoPromedio("tablon.tsv", List("100070934", "100070905"), "201501", "201512")
    z.show()

  }

}