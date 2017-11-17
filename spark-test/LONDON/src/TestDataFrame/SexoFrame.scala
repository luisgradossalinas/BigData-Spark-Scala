package TestDataFrame

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SexoFrame {

  /*
  var spark = SparkSession
    .builder
    .appName("PocDF")
    .master("local[*]")
    .getOrCreate()
    */

  var sc = new SparkContext("local[*]", "SexoFrame")
  var sqlContext = new org.apache.spark.sql.SQLContext(sc)

  def byClientes(ruta: String, esta: List[String], inicio: String, fin: String): DataFrame = {

    val rddTablon = sc.textFile(ruta).map(r => r.split("\t")).map(r => Row(r(6), r(25), r(43)))
    val cabeceras = "CODESTABLECIMIENTO CODMES SEXO_CLIENTE"
    val camposDF = cabeceras.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(camposDF)
    
    val tablonDF = sqlContext.createDataFrame(rddTablon, schema)
    return tablonDF.filter((tablonDF("CODESTABLECIMIENTO") isin (esta: _*)) && (!tablonDF("SEXO_CLIENTE").equalTo("\\N")) && (tablonDF("CODMES").between(inicio, fin)))
      .groupBy("SEXO_CLIENTE").agg(count("SEXO_CLIENTE").as("TOTAL"))

  }

  def evolucionCompras(ruta: String, esta: List[String], inicio: String, fin: String): DataFrame = {

    val rddTablon = sc.textFile(ruta).map(r => r.split("\t")).map(r => Row(r(6), r(25), r(43), r(12).toDouble))

    val schema = new StructType()
      .add("CODESTABLECIMIENTO", StringType, true)
      .add("CODMES", StringType, true)
      .add("SEXO_CLIENTE", StringType, true)
      .add("MTOTRANSACCION", DoubleType, true)

    val tablonDF = sqlContext.createDataFrame(rddTablon, schema)
    return tablonDF.filter((tablonDF("CODESTABLECIMIENTO") isin (esta: _*)) && (!tablonDF("SEXO_CLIENTE").equalTo("\\N")) && (tablonDF("CODMES").between(inicio, fin)))
      .groupBy("CODMES", "SEXO_CLIENTE").agg(sum("MTOTRANSACCION").as("MONTO_TOTAL")).orderBy("CODMES", "SEXO_CLIENTE")

  }

  def montoPromedio(ruta: String, esta: List[String], inicio: String, fin: String): DataFrame = {

    val rddTablon = sc.textFile(ruta).map(r => r.split("\t")).map(r => Row(r(6), r(25), r(43), r(12).toDouble))

    val schema = new StructType()
      .add("CODESTABLECIMIENTO", StringType, true)
      .add("CODMES", StringType, true)
      .add("SEXO_CLIENTE", StringType, true)
      .add("MTOTRANSACCION", DoubleType, true)

    val tablonDF = sqlContext.createDataFrame(rddTablon, schema)
    return tablonDF.filter((tablonDF("CODESTABLECIMIENTO") isin (esta: _*)) && (!tablonDF("SEXO_CLIENTE").equalTo("\\N")) && (tablonDF("CODMES").between(inicio, fin)))
      .groupBy("CODMES", "SEXO_CLIENTE").agg(avg("MTOTRANSACCION").as("MONTO_PROMEDIO")).orderBy("CODMES", "SEXO_CLIENTE")

  }

  def main(args: Array[String]) {

    val x = byClientes("tablon.tsv", List("100070934", "100070905"), "201501", "201512")
    //x.show()
    x.toJSON.collect().foreach(println)
    
    
    val y = evolucionCompras("tablon.tsv", List("100070934", "100070905"), "201501", "201512")
    //y.show()
    y.toJSON.collect().foreach(println)
    
    val z = montoPromedio("tablon.tsv", List("100070934", "100070905"), "201501", "201512")
    //z.show()
    z.toJSON.collect().foreach(println)


  }

}