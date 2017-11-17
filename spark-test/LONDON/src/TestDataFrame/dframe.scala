package TestDataFrame

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object dframe {

  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", "dframe")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    
    val peopleRDD = sc.textFile("tablon.tsv")
    //Definir el nombre de los campos que tendrá el DataFrame
    val schemaString = "CODESTABLECIMIENTO CODMES SEXO_CLIENTE"
    // Generando las columnas que tendrá el DataFrame
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    // Convertir registros del RDD en Filas para el DataFrame
    val rowRDD = peopleRDD.map(_.split("\t")).map(attributes => Row(attributes(6), attributes(25),attributes(43)))
    //Creando un DataFrame a partir de un RDD
    val peopleDF = sqlContext.createDataFrame(rowRDD, schema)
    //peopleDF.show()

    // Creates a temporary view using the DataFrame
    //peopleDF.createOrReplaceTempView("tablon")
    // SQL can be run over a temporary view created using DataFrames
    //val results = sqlContext.sql("SELECT distinct SEXO_CLIENTE FROM tablon")
    //results.show()
    
    //peopleDF.filter(peopleDF("SEXO_CLIENTE") == 'F').count().show()
    
    println("Registros mujeres: " + peopleDF.filter(peopleDF("SEXO_CLIENTE").equalTo("F")).count())
    println("Registros hombres: " + peopleDF.filter(peopleDF("SEXO_CLIENTE").equalTo("M")).count())
    
    
    
    peopleDF.filter(peopleDF("SEXO_CLIENTE").contains("123")).show()
    
    
    
    
    
    
    /*
    val dfs = sqlContext.read.json("employee.json")
    //sqlContext.createDataFrame(rdd, beanClass)
    dfs.show()
    dfs.printSchema()
    //Solo mostrar una columna
    dfs.select("name").show()
    //Filtrar el DataFrame
    dfs.filter(dfs("age") > 28).show()
    //Agrupados por edad
    //val x = dfs.groupBy("age").count()
    //dfs.groupBy("age").sum("age").show()
    //x.show()
    dfs.select(dfs("name"), dfs("age") + 1).show()
    
    dfs.groupBy("id").sum("age")
    */
    

  }

}