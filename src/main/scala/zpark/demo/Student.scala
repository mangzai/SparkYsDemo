package zpark.demo

class Student(var id:Int,var name:String,avg:Int) {
   //val id=9527
  //var name="tbh"
  def myprint=println(avg)
  println("hello scala")
}
object Student{
  def main(args:Array[String]):Unit={
    val student: Student = new Student(1,"nihao",23)
    println(student.id)
    println(student.name)
  }
}