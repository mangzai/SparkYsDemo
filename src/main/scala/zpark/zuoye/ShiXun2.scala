package zpark.zuoye

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author ys
 *         data 2020/6/23 10:24
 */
object ShiXun2 {
  val hdfsPath = "hdfs://hdp-101:9000/access333_log/20-06-23/18-00"

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ETL")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val lines: RDD[String] = sparkSession.sparkContext.textFile(hdfsPath)
    val lines1: RDD[String] = lines.filter(_.size > 0)
    lines1.foreach(println)
    val ips: RDD[String] = lines1.map(line => line.split(" ")).map(arr => arr(0))
    ips.foreach(println)
    val tup: RDD[(String, Int)] = ips.map(ip => (ip, 1))
    val reduced: RDD[(String, Int)] = tup.reduceByKey((x, y) => (x + y))
    // val reduced: RDD[(String, Int)] = tup.reduceByKey(_ + _)
    reduced.foreach(println)
    val ipdone: RDD[ip] = reduced.map(a => {
      ip(a._1, a._2)
    })
    import sparkSession.implicits._
    val df: DataFrame = ipdone.toDF()
    val table = "ip1"
    val url = "jdbc:mysql://47.100.185.251:3306/db_spark?charactorEncoding=utf-8&serverTimezone=UTC"
    val props = new Properties()
    props.put("user", "root")
    props.put("password", "rootroot")
    props.put("driver", "com.mysql.jdbc.Driver")
   // df.write.mode("append").jdbc(url, table, props)
    df.write.mode("").jdbc(url, table, props)
    df.show( )
  }

  case class ip(ip: String, count: Int)

}
