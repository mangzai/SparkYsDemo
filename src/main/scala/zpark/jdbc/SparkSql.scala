package zpark.jdbc

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author ys
 * @date 2020/4/28  21:57
 */
object SparkSql {
  def main(args: Array[String]): Unit = {
    //创建配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql")
    //sparkSession
    //创建SparkCSQL的环境对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val frame: DataFrame = spark.read.json("in/people.json")
    //展示数据
    frame.show()
    //释放资源
    spark.close()
  }

}
