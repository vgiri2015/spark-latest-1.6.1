package learning

/**
 * Created by gfp2ram on 9/4/2015.
 */
object FunctionTest {
  def main(args:Array[String]): Unit = {
    println("Sum is =",addInt(5,7))
    printThis()
    delayed(time())
    println("Factorial Values",factorial(0))
    println("Factorial Values",factorial(1))
    println("Factorial Values",factorial(2))
    println("Factorial Values",factorial(3))

  }
  /*Function with Return Type Int*/
  def addInt(a: Int, b: Int): Int = {
    var sum: Int=0
    sum=a+b
    return sum
  }
  /*Functions with No Return Type*/
  def printThis() : Unit = {
    println("Welcome to Functional Programming Language")
  }
  /*Calling time function*/
  def time() = {
    println("Getting time in Nano Seconds")
    System.nanoTime()
  }
  /*Calling the function delayed which has return type of Long based on the return value of time() function*/
  def delayed(t: => Long) : Unit = {
    println("Inside Delayed Method")
    println("Param",+ t)
  }
  /*Calling Recursively*/
  def factorial(i:Int) : Int = {
    def fact(i:Int,accumulator:Int) : Int = {
      if(i<=1)
        accumulator
      else
        fact(i-1,i*accumulator)
    }
    fact(i,1)
  }
}
