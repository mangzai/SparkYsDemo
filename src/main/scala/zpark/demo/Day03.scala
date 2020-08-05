package zpark.demo

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Day03 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List[String]("lovel", "love2", "love3", "love4", "love5", "love6"), 3)
    val rdd2=rdd1.mapPartitionsWithIndex((index,iter)=>{
      val list=new ListBuffer[String]()
      while(iter.hasNext){
        val one=iter.next()
        list.+=(s"rdd1 partition=[$index],value=[$one]")
      }
      list.iterator
    })
    val rdd3 = rdd2.coalesce(4,true)
    println(s"rdd3 partition length=${rdd3.getNumPartitions}")
    val rdd4=rdd3.mapPartitionsWithIndex((index,iter)=>{
      val list=new ListBuffer[String]()
      while(iter.hasNext){
        val one=iter.next()
        list.+=(s"rdd3 partition=[$index],value=[$one]")

      }
      list.iterator
    })
    val result=rdd4.collect()
    result.foreach(println)
  }

}
