package zpark.sparkDemo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object reduceDemo {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setAppName("ta")
    conf.setMaster("local")
    val sc=new SparkContext(conf)
    val input:RDD[Int]=sc.parallelize(List(1,2,3,4))
    val res=input.reduce((x,y)=>x+y)

    println(res)


  }

}
