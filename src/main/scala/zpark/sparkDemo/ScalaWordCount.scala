package zpark.sparkDemo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {
  def main(args: Array[String]): Unit = {
    //1.做出ｓｃ
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("wc")
    val sc = new SparkContext(conf)
    //println("nihao")
    //2.操作ｓｃ，做出ＲＤＤ
    val lines:RDD[String] = sc.textFile("hdfs://hdp-101:9000/color.txt")
    //3.操作ＲＤＤ　count
    val resCount:Long = lines.count()
    println(resCount)



  }
}
