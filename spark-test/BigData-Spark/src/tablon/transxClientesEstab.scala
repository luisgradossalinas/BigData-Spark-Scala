package tablon

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object transxClientesEstab {
  
  
def kpi_establecimientos_transacciones_x_clientes(ruta: String) = {
   val sc = new SparkContext("local[*]", "transxClientesEstab")
	val rdd = sc.textFile(ruta)
	val r1 = rdd.map(r => r.split("\t")).map(r => (r(6),r(14)))
	//Tenemos dos alternativas
	r1.sortByKey().map(x => (x._1+","+x._2,1)).reduceByKey(_+_).foreach(println)
	/*
	(508565949,16637160,3)
  (100128028,219140,1)
  (100313202,1918059,1)
	 **/
	//r1.sortByKey(true).countByValue.foreach(println)
	  /*
((100070128,1045370),1)
((503840109,11429123),1)
((100070014,13649832),2)
     * */
}

   def main(args: Array[String]) {
   
      kpi_establecimientos_transacciones_x_clientes("tablon.tsv")
  
  }
  
  
}