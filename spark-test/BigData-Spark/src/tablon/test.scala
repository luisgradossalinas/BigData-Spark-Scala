package tablon

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object test {
  
   def main(args: Array[String]) {
    
    val sc = new SparkContext("local[*]", "test")
    
     val c = sc.parallelize(Array(("est1", 1), ("est1", 1),("est1", 2)))
     c.reduceByKey(_+_).foreach(println) //(est1,4)
     c.map(x => (x._1+"-"+x._2,1)).reduceByKey(_+_).foreach(println)
     //(est1-2,1)
     //(est1-1,2)
     
     
     c.map(x => (x,x._1)).foreach(println) //Muestra todo el arreglo 
     //((est1,2),est1)
     //((est1,1),est1)
     //((est1,1),est1)
     
     println("Test")
     println(c.map(x => (x._2)).reduce(_+_)) //4 Suma todos los valores de y
     
    
     c.groupByKey().foreach(println) //(est1,CompactBuffer(1, 1, 2))
     
     
     val dataAlumnos = sc.parallelize(Array(("Martin", 20, 14), ("Iveth", 17,18),("Jorge", 15,18)))
     
     //dataAlumnos.map(x => (x._1,(x._2.toInt/x._3.toInt).toFloat)).foreach(println)
     dataAlumnos.map(x => (x._1,(x._2.toFloat+x._3.toFloat)/2.toFloat)).sortByKey().foreach(println) //Lo ordena por el nombre del alumno
     //(Iveth,17.5)
     //(Jorge,16.5)
     //(Martin,17.0)
   
     
     val dataBD = sc.parallelize(Array(("Sudamérica","Perú",30),("Sudamérica","Argentina",25),("Sudamérica","Colombia",20),("Europa","España",10)))
     println("Habitantes por continentes")
     dataBD.map(x => (x._1,x._3)).reduceByKey(_+_).foreach(println) //Se suma el campo x.3 por continente
     println("Habitantes por países")
     dataBD.map(x => (x._2,x._3)).reduceByKey(_+_).foreach(println)
     println("Países por continente")
     dataBD.map(x => (x._1,1)).reduceByKey(_+_).foreach(println) //Cuando se pone 1 se realiza un count de registros
     
     
     val rdd1 = sc.parallelize(Array(("Martin",28),("Iveth",28),("Jorge",33)))
     val rdd2 = sc.parallelize(Array(("Martin",4600),("Iveth",2500),("Jorge",8000)))
     
     //rdd1.union(rdd2).foreach(println)
     /*
      (Jorge,33)
      (Martin,28)
      (Iveth,28)
      (Martin,4600)
      (Jorge,8000)
      (Iveth,2500)
      */
     //Uso de Intersection
     println("Uso de intesection")
     rdd1.intersection(rdd2).foreach(println) //Se intersectan siempre y cuando tengan los mismos valores
     
     //Agrupa cuando los keys son iguales
     val joinRdd = rdd1.join(rdd2)
     /*
      * (Jorge,(33,8000))
        (Martin,(28,4600))
        (Iveth,(28,2500))
      */
     
     //Sumar los 2 valores del join
     joinRdd.mapValues(x => (x._1.toFloat + x._2.toFloat)).foreach(println)
     /*
     (Iveth,2528.0)
    (Jorge,8033.0)
    (Martin,4628.0)
     */
     
     val dataCurso = sc.parallelize(List(("Matemática",40,2),("Lenguaje I",50,3),("Computación",55,2),("Lenguaje II",20,4)))
     
     println("Búsqueda 1")
     dataCurso.filter(x => x._2 >= 40 && x._2 <= 50 && x._1.contains("Matemá")).foreach(println)
     println("Búsqueda 2")
     dataCurso.filter(x => x._1.startsWith("Mate")).foreach(println)
     println("Búsqueda 3")
     dataCurso.filter(x => x._1.contains("Matemá")).foreach(println)
         
     
  }
  
}