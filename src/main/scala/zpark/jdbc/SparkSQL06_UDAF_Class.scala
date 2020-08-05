package zpark.jdbc

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator

/**
 * @author ys
 * @date 2020/5/4  12:31
 */
object SparkSQL06_UDAF_Class {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL06_UDAD_Class")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //引入隐式转换
    import spark.implicits._
    //将聚合函数转换为查询列
    val udaf = new MyAgeAvgClassFunction
    val avgCol: TypedColumn[UserBean, Double] = udaf.toColumn.name("avgAge")
    val frame: DataFrame = spark.read.json("hdfs://hdp-101:9000/people.json")
    val userDS: Dataset[UserBean] = frame.as[UserBean]
    //应用函数
    userDS.select(avgCol).show()
    //释放资源
    spark.stop()
  }

}

case class UserBean(name: String, age: BigInt)

case class AvgBuffer(var sum: BigInt, var count: Int)

class MyAgeAvgClassFunction extends Aggregator[UserBean, AvgBuffer, Double] {
  //初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  //聚合数据
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  //缓冲区的合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  //完成计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = {
    Encoders.product
  }

  override def outputEncoder: Encoder[Double] = {
    Encoders.scalaDouble
  }
}