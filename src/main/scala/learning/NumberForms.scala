package learning

// To execute Scala, please define an object named Solution that extends App

object NumberForms {
  def main(args:Array[String]): Unit = {
    val a = Array(5, 1, 2, 3, 4)
    val b = Array(6, 1, 3, 4, 5)
    val c = numberForms(a)
    println(c)
  }
  def numberForms(nums: Array[Int]): Int = {
    return nums(2)+nums(4)
  }
}


