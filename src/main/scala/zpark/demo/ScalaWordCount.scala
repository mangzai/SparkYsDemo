package zpark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {
  def main(args: Array[String]): Unit = {
    //1.做出sc
    val conf: SparkConf = new SparkConf()
   // conf.setMaster("local[*]")
    conf.setAppName("wc")
    val sc: SparkContext = new SparkContext(conf)
    //2.操作sc，做出RDD
    val lines: RDD[String] = sc.textFile("hdfs://hdp-101:9000/hi.txt")
    //val lines: RDD[String] = sc.textFile(path = "in/hi.txt")

  /* //3.操作RDD count
    val resCount: Long = lines.count()
    println(resCount)*/

    //4.字数统计
    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordAndone: RDD[(String, Int)] = words.map((_, 1))

    val wordAndCount: RDD[(String, Int)] = wordAndone.reduceByKey((_ + _))

    wordAndCount.foreach(println)
    wordAndCount.saveAsTextFile("hdfs://hdp-101:9000/out/out1")
   // wordAndCount.saveAsTextFile("out1/in")

    sc.stop()
  }

}
