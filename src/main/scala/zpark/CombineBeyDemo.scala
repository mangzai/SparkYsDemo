package zpark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CombineBeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("CombineBeyKey")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("hdfs://hdp-101:9000/color.txt")
 //   val input: RDD[(String, Int)] = lines.map(x =>(x.split(" ")(0), x.split(" ")(1).toInt))
    val input: RDD[(String, Int)] = lines.map(x=>(x.split(" ")(0),x.split(" ")(1).toInt))

val result = input.combineByKey((v) => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map{ case (key, value) => (key, value._1 / value._2.toFloat) }
    result.collectAsMap().map(println(_))


  }

}
