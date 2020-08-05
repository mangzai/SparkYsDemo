package zpark.txt

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object Spark_Json {
  def main(args : Array[String]):Unit={
    val sc: SparkContext = initSparkContext
    val json: RDD[String] = sc.textFile("E:\\IdeaProject\\sparkDemo\\src\\main\\scala\\com\\zpark\\people.json")
    val result: RDD[Option[Any]] = json.map(JSON.parseFull)
    result.foreach(println)
    sc.stop()
  }

  private def initSparkContext = {
    val conf: SparkConf = new SparkConf().setAppName("spark_Json").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc
  }
}
