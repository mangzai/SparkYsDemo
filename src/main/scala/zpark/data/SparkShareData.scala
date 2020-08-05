package zpark.data

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 * @date 2020/4/14  8:52
 */
object SparkShareData {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 3, 4, 4),2 )
    //val i: Int = dataRDD.reduce((x: Int, y: Int) => x + y)
    //println(i)
    var sum=0
    dataRDD.foreach(i=>sum=sum+i)
    println("sum=" + sum)
  }

}
