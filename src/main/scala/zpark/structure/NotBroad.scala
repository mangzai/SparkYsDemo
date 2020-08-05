package zpark.structure

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 * @date 2020/4/22  9:26
 */
object NotBroad {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = initSpark

    val info1: RDD[(String, String)] = sc.parallelize(Array(("101", "张三"), ("102", "李四")))
    val info2: RDD[(String, (String, String))] = sc.parallelize(Array(("101", "王五", "12"), ("102", "小六", "25")))
      .map(x => (x._1, (x._2, x._3))) //转换成元组(101,(王五，12))
    info1.join(info2).map(x => {
      x._1 + "," + x._2._1 + "," + x._2._2._1
    }).foreach(println)
  }


  private def initSpark = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Broad")
    val sc = new SparkContext(conf)
    sc
  }
}
