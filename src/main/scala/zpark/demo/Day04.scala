package zpark.demo

import org.apache.spark.{SparkConf, SparkContext}

object Day04 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List("zhangsan", "lisi", "wangwu", "maliu"))
    val rdd2 = sc.parallelize(List("300", "400", "500", "600"))
    val result = rdd1.zip(rdd2)
    val result1=rdd1.zipWithIndex()
    result1.foreach(println)
    result.foreach(println)

  result.saveAsTextFile("E:\\IdeaProject\\sparkDemo\\src\\main\\scala\\com\\zpark\\demo\\hei.txt")
  }

}
