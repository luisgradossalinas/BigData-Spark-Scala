package tablon

object fechas {
  
  
  def calculoFecha (fecha:String,num:Int) = {
    
    //val fec1 = fecha
    val ano = fecha.substring(0,4)
    val mes = fecha.substring(4,6).toInt

    val t = ano.toString()+mes.toString()
    
    //val x = mes-num 
    
    //var x = 
    /*
    for(mes <- num ){
      

    }
    * */
    
    
    /*
    
    if (x <=0) {
      println(x)
      var y = 12 - x
      if (y 
    } else {
      println(x)
    }
    * */

    
  }
  
  
  
  def main(args: Array[String]) {
    
    calculoFecha("201707",8)
    
    //Tres meses -> 201705
    //Seis meses -> 2017
    
    
  }
    

  
}