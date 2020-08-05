package zpark.txt

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
 * @author ys
 * @date 2020/4/23  8:03
 */
object JsonDemo1 {
  def main(args:Array[String]):Unit={
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val sc = new SparkContext(conf)
    val json: RDD[String] = sc.textFile("in/people.json")
    val result: RDD[Option[Any]] = json.map(JSON.parseFull)
    result.foreach(println)
    sc.stop()
  }

}
