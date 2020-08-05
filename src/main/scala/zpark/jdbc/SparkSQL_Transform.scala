package zpark.jdbc

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author ys
 * @date 2020/5/4  10:12
 */
object SparkSQL_Transform {
  def main(args: Array[String]): Unit = {
    //sparkSQL
    //sparkConf
    //创建配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_Transform")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //进行转换之前，需要引入隐式转换规则
    import spark.implicits._
    //这里的spark不是包名的含义，是SparkSession对象的名字
    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 21)))

    //转换为DF
    val df: DataFrame = rdd.toDF("id", "name", "age")
    //转换为DS
    val ds: Dataset[User] = df.as[User]
    //转换为DF
    val df1: DataFrame = df.toDF()
    //转换为RDD
    val rdd1: RDD[Row] = df1.rdd
    rdd1.foreach(row => {
      println(row.getString(1))
      println(row.getInt(0))
    })

    //释放资源
    spark.stop()
  }

}

case class User(id: Int, name: String, age: Int)