package zpark.zuoye

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author ys
 *         data 2020/6/23 10:07
 */
object ShiXun1 {
  val hdfsPath = "hdfs://hdp-101:9000/access333_log/20-06-23/18-00"

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ETL")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val lines: RDD[String] = sparkSession.sparkContext.textFile(hdfsPath)
    lines.foreach(print)
    val splited: RDD[String] = lines.map(line => line.split(" ")).map(arr => (arr(0)))
    val ip: RDD[Boolean] = splited.map(ips => ips.startsWith("1")).filter(a => a.equals(true))
    ip.count()
    splited.foreach(print)
  }

}
