package learning

class Foo {
  val sampleString = "Hi there, How are you. %s"
  def printThisFunction(name:String):String = {
    val outputStr = this.sampleString
    return outputStr.format(name)
  }
}
object FetchFoo {
  def main(args:Array[String]):Unit ={
    //val foo = Class.forName("Foo").newInstance().asInstanceOf[{def printThisFunction (name:String):String}]
    //println(foo.printThisFunction("This is Excellent SPark"))
    val foo1 = new Foo
    println(foo1.printThisFunction("Giri is working now in Spark"))
  }
}