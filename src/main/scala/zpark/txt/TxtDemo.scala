package zpark.txt

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TxtDemo {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = initSpark
    val localPath:String="E:\\IdeaProject\\sparkDemo\\src\\main\\scala\\com\\zpark\\a.txt"
    val lines: RDD[String] = sc.textFile(localPath)
    lines.foreach(println)
    println("--------------------------------")
    val liness: RDD[(String, String)] = sc.wholeTextFiles(localPath)
    liness.foreach(println)
    lines.saveAsTextFile("output")
    liness.saveAsTextFile("output1")


  }

  private def initSpark():SparkContext = {
    val conf: SparkConf = new SparkConf().setAppName("txtDemo").setMaster("local[*]")
    val sc= new SparkContext(conf)
    sc
  }
}
