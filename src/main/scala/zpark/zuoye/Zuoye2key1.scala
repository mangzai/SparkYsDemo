package zpark.zuoye

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 *         data 2020/6/17 12:46
 */
object Zuoye2key1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ETL")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("in/ti2.txt")
    //groupByKey
    val maped: RDD[Array[String]] = lines.map(_.split(" "))
    val mapped: RDD[(String, String)] = maped.map(x => (x(0), x(1)))
    val grouped: RDD[(String, Iterable[String])] = mapped.groupByKey()
    //分组
    val result: RDD[(String, List[String])] = grouped.map {
      x => {
        val key: String = x._1
        val value: Iterable[String] = x._2
        (key, value.toList.sorted.reverse.take(2))
      }
    }

    println(result.collect.toList)
  }
}
