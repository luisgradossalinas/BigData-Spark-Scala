package KPI

import java.util.ArrayList

object test {

  def main(args: Array[String]) {

    var arr = Array(1, 3, 6, 12, 24)
    var v = new Array[String](arr.length)

    var contador: Int = 0

    for (name <- arr) {

      v(contador) = "martin" + arr(contador)
      println(v(contador))
      contador = contador + 1

    }

  }
}