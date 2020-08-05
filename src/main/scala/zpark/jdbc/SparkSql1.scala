package zpark.jdbc

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author ys
 * @date 2020/4/28  22:57
 */
object SparkSql1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSql1")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val frame: DataFrame = spark.read.json("in/people.json")
    //将DataFrame转换成为一张表
    frame.createTempView("user")
    //采用 sql的语法访问数据
    spark.sql("select * from user").show()
    //释放资源
    spark.stop()
  }

}
