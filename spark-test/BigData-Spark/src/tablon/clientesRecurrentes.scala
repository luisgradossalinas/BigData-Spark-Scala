package tablon

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import java.util.ArrayList

object clientesRecurrentes {
  
  def kpi_clientes_recurrentes(ruta: String,establecimiento:String,inicio:String, fin:String) = {
      
  val sc = new SparkContext("local[*]", "clientesRecurrentes")
  val rangoVisitas = List("1","2","3_5","")
  
	val rddTablon = sc.textFile(ruta)
	val rddDepurado = rddTablon.map(r => r.split("\t")).map(r => (r(6),r(14),r(25)))
	val rddFiltrado = rddDepurado.filter(x => (x._3 >= inicio && x._3 <= fin) && x._1 == establecimiento).map(x => (x._1+"-"+x._2,1)).reduceByKey(_+_)
	//val rddXclientes = rddFiltrado.map(x => (x._1+"-"+x._2,1)).reduceByKey(_+_)
	//val rddVisitas_Frecuencia1 = rddFiltrado.filter(x => x._2 == 1).map(x => (x._1.split("-")(0)+"-1",1)).reduceByKey(_+_)
	//val rddVisitas_Frecuencia2 = rddFiltrado.filter(x => x._2 == 2).map(x => (x._1.split("-")(0)+"-2",1)).reduceByKey(_+_)
	//val rddVisitas_Frecuencia3_5 = rddFiltrado.filter(x => x._2 >= 3 & x._2 <=5).map(x => (x._1.split("-")(0)+"-3_5",1)).reduceByKey(_+_)
	//val rddVisitas_Frecuencia6_10 = rddFiltrado.filter(x => x._2 >= 6 & x._2 <=10).map(x => (x._1.split("-")(0)+"6_10",1)).reduceByKey(_+_)
	//val rddVisitas_Frecuencia11_20 = rddFiltrado.filter(x => x._2 >= 11 & x._2 <=20).map(x => (x._1.split("-")(0)+"11_20",1)).reduceByKey(_+_)
	//val rddVisitas_Frecuencia21_mas = rddFiltrado.filter(x => x._2 >= 21).map(x => (x._1.split("-")(0)+"21_mas",1)).reduceByKey(_+_)
	
	val rddVisitas_Frecuencia1 = rddFiltrado.filter(x => x._2 == 1).map(x => ("1",1)).reduceByKey(_+_)
	val rddVisitas_Frecuencia2 = rddFiltrado.filter(x => x._2 == 2).map(x => ("2",1)).reduceByKey(_+_)
	val rddVisitas_Frecuencia3_5 = rddFiltrado.filter(x => x._2 >= 3 & x._2 <=5).map(x => ("3_5",1)).reduceByKey(_+_)
	val rddVisitas_Frecuencia6_10 = rddFiltrado.filter(x => x._2 >= 6 & x._2 <=10).map(x => ("6_10",1)).reduceByKey(_+_)
	val rddVisitas_Frecuencia11_20 = rddFiltrado.filter(x => x._2 >= 11 & x._2 <=20).map(x => ("11_20",1)).reduceByKey(_+_)
	val rddVisitas_Frecuencia21_mas = rddFiltrado.filter(x => x._2 >= 21).map(x => ("21_mas",1)).reduceByKey(_+_)
	
	//Unimos 
	val visitasAgrupadas = rddVisitas_Frecuencia1.union(rddVisitas_Frecuencia2).union(rddVisitas_Frecuencia3_5)
	  .union(rddVisitas_Frecuencia6_10).union(rddVisitas_Frecuencia11_20).union(rddVisitas_Frecuencia21_mas)
	
	println("Registros detallados")
	rddFiltrado.foreach(println)
	println("Número de registros")
	println(rddFiltrado.count())
	println("Agrupación por clientes")
	rddFiltrado.foreach(println)
	println(rddFiltrado.count())

	visitasAgrupadas.foreach(println)
	 
    }
  
  def kpi_clientes_recurrentes_n_establecimientos(ruta: String,est1:String,est2:String,inicio:String, fin:String) = {
      
  val sc = new SparkContext("local[*]", "clientesRecurrentes")
  val rangoVisitas = List("1","2","3_5","")
  
	val rddTablon = sc.textFile(ruta)
	val rddDepurado = rddTablon.map(r => r.split("\t")).map(r => (r(6),r(14),r(25)))
	val rddFiltrado = rddDepurado.filter(x => (x._3 >= inicio && x._3 <= fin) && (x._1 == est1 || x._1 == est2)).map(x => (x._1+"-"+x._2,1)).reduceByKey(_+_)

	val rddVisitas_Frecuencia1 = rddFiltrado.filter(x => x._2 == 1).map(x => ("1",1)).reduceByKey(_+_)
	val rddVisitas_Frecuencia2 = rddFiltrado.filter(x => x._2 == 2).map(x => ("2",1)).reduceByKey(_+_)
	val rddVisitas_Frecuencia3_5 = rddFiltrado.filter(x => x._2 >= 3 & x._2 <=5).map(x => ("3_5",1)).reduceByKey(_+_)
	val rddVisitas_Frecuencia6_10 = rddFiltrado.filter(x => x._2 >= 6 & x._2 <=10).map(x => ("6_10",1)).reduceByKey(_+_)
	val rddVisitas_Frecuencia11_20 = rddFiltrado.filter(x => x._2 >= 11 & x._2 <=20).map(x => ("11_20",1)).reduceByKey(_+_)
	val rddVisitas_Frecuencia21_mas = rddFiltrado.filter(x => x._2 >= 21).map(x => ("21_mas",1)).reduceByKey(_+_)
	
	//Unimos 
	val visitasAgrupadas = rddVisitas_Frecuencia1.union(rddVisitas_Frecuencia2).union(rddVisitas_Frecuencia3_5)
	  .union(rddVisitas_Frecuencia6_10).union(rddVisitas_Frecuencia11_20).union(rddVisitas_Frecuencia21_mas)
	
	println("Registros detallados")
	rddFiltrado.foreach(println)
	println("Número de registros")
	println(rddFiltrado.count())
	println("Agrupación por clientes")
	rddFiltrado.foreach(println)
	println(rddFiltrado.count())

	visitasAgrupadas.foreach(println)
	

    }
  
  
  def main(args: Array[String]) {
    
    //kpi_clientes_recurrentes("tablon.tsv","100070934","201609","201610")
    kpi_clientes_recurrentes_n_establecimientos("tablon.tsv","100070934","100070905","201609","201610")

  }
  
}