package zpark.txt

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 * @date 2020/4/16  8:36
 */
object DeadHdfsDemo {

  private def initSpark = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HDfs")
    val sc: SparkContext = new SparkContext(conf)
    sc
  }

  def main(args: Array[String]): Unit = {
    val hdfsFilePath: String = "hdfs://hdp-101:9000/hi.txt"

    val sc: SparkContext = initSpark
    val lines: RDD[String] = sc.textFile(hdfsFilePath)
    lines.foreach(println)
  }

}
