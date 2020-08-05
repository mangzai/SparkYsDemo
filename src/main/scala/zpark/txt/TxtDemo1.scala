package zpark.txt

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object TxtDemo1 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = initspark
    val lines: RDD[String] = sc.textFile("E:\\IdeaProject\\sparkDemo\\src\\main\\scala\\com\\zpark\\people.json")
    //对lines进行json解析
    //Option有数据是Some对象，没有数据是Nome对象
    val result: RDD[Option[Any]] = lines.map(line => JSON.parseFull(line))
    val filtered: RDD[Option[Any]] = result.filter {
      case Some(map: Map[String, Any]) => true
      case None => false
    }
    filtered.foreach(println)

    filtered.saveAsTextFile("e:/sparkData/out/jsonout")
    result.foreach(
      r=>r match {
        case Some(map:Map[String,Any])=>println(r)
        case None=>println("error")
        case other=>println("unkonw error"+other)
      }
    )
  }

  private def initspark: SparkContext = {
    val conf: SparkConf = new SparkConf().setAppName("txtDemo1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc
  }
}
