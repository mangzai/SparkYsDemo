package zpark.zuoye

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

/**
 * @author ys
 *         data 2020/6/30 10:33
 */
object ShiXun3 {
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ETL")
  val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  private val sc: SparkContext = sparkSession.sparkContext
  val sqlContext: SQLContext = sparkSession.sqlContext
  val df: DataFrame = sparkSession.read.format("json").load("in/index.json")
  df.createOrReplaceTempView("totiao")
  val sql1 = " select result.data.date from totiao"
  val sql2 = "select result.data.author_name from totiao"

  def main(args: Array[String]): Unit = {
    toCount


  }


  private def toCount = {
    val rdd: RDD[String] = sqlContext.sql(sql2).rdd.map(_.mkString(","))
    val word: RDD[String] = rdd.flatMap(line => line.split(","))
    val count: RDD[(String, Int)] = word.map(x => (x, 1)).reduceByKey((x, y) => x + y).sortBy(x => x._2, false)
    count.foreach(println)
    count
  }
}
