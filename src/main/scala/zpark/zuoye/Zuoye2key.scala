package zpark.zuoye

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 *         data 2020/6/17 12:24
 */
object Zuoye2key {
  def main(args: Array[String]): Unit = {
    /*需求：11111111111111111111111111111111111111111
    1、分组
    2、排序
    3、取出值最大的前两条数据*/
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ETL")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("in/ti2.txt")
    val rdd1: RDD[(String, Int)] = lines.map(line => (line.split(" ")(0), line.split(" ")(1).toInt))
    val result1: RDD[(String, Iterable[Int])] = rdd1.groupByKey()
    result1.foreach(println)
    result1.foreach(
      line => {
        val key: String = line._1
        val value: List[Int] = line._2.toList.reverse
        //val value1: List[Int] = line._2.toList.reverse.take(2)

        println(key + ":" + value(0), value(1))
       // println(key + ":" + value1)
      }
    )
  }

}
