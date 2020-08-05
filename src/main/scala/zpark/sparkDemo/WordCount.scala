package zpark.sparkDemo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象，设定spark计算框架运行环境
    val config:SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建spark上下文对象
    val sc=new SparkContext(config)
    //读取文件
    val lines:RDD[String]=sc.textFile("in/hi.txt")
    //将数据分解
    val word :RDD[String]=lines.flatMap((_.split(" ")))
    val wordToOne:RDD[(String,Int)]=word.map((_,1))
    val wordToSum:RDD[(String,Int)]=wordToOne.reduceByKey(_+_)
    val tuples:Array[(String,Int)]=wordToSum.collect()
    tuples.foreach(it=>print(it))
  }
}
