package zpark.txt

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.parsing.json.JSON

object JsonDemo {
  val line = "{\"name\":\"Justin\", \"age\":19}"
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = initSpark
    //读取json数据并解析
    val lines: RDD[String] = sc.textFile("E:\\IdeaProject\\sparkDemo\\src\\main\\scala\\com\\zpark\\people.json")

    parseJsonOne
   // parseJsonTwo(lines)

//
  }

  private def parseJsonTwo(lines: RDD[String]) = {
    //对lines进行json解析
    //Option中有两种情况：1、里面有数据的话就是一个Some对象，2、没有数据的话就是一个Nome对象
    val result: RDD[Option[Any]] = lines.map(line => JSON.parseFull(line))
    val filtered: RDD[Option[Any]] = result.filter {
      case Some(map: Map[String, Any]) => true
      case None => false
    }
    filtered.foreach(print)
    //保存
    //filtered.saveAsTextFile("e:/sparkData/out/jsonout")
    //    result.foreach(
    //      r => r match {
    //        case Some(map:Map[String,Any]) => println(r)
    //        case None => println("error")
    //        case other => println("unknow error" + other)
    //      }
    //    )
  }

  private def parseJsonOne = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val person: Person = mapper.readValue(line, classOf[Person])
    println(person)
  }

  def initSpark: SparkContext = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("jsondemo")
    val sc = new SparkContext(conf)
    sc
  }
}

case class Person(name:String,age:java.lang.Integer)

