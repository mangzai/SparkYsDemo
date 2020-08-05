package zpark.sparkDemo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Funck {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setMaster("local")
    conf.setAppName("ta")
    val sc=new SparkContext(conf)
    val input:RDD[Int]=sc.parallelize(List(3,4,52,3,4))
    val result:RDD[Int]=input.map(x=>x*x)
    println("result++++++++++++++++++++++++++++++++++"+result)
    val result2:RDD[Int]=input.flatMap(x=>x.to(3))
    //过滤操作
   val filertest= input.filter(x=>x!=3)
    println(filertest.collect().mkString(","))
    //flatMap()切割操作
    println(result.collect().mkString(","))
  }

}
