class ClassOne[T](val input: T) {}

class ClassOneStr(val one: ClassOne[String]) {
  def duplicatedString() = one.input + one.input
}

class ClassOneInt(val one: ClassOne[Int]) {
  def duplicatedInt = one.input.toString + one.input.toString
}

implicit def toStr(one: ClassOne[String]) = new ClassOneStr(one)
implicit def toInt(one: ClassOne[Int]) = new ClassOneInt(one)

val oneStrTest = new ClassOne("test")
val oneIntTest = new ClassOne(123)

oneStrTest.duplicatedString
oneIntTest.duplicatedInt