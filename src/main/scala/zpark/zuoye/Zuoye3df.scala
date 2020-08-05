package zpark.zuoye

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
 * @author ys
 *         data 2020/6/18 19:06
 */
object Zuoye3df {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ETL")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    val sqlContext: SQLContext = sparkSession.sqlContext
    val lines: RDD[String] = sc.textFile("in/ti3.txt")
    val spalited: RDD[Array[String]] = lines.map(line => line.split(":"))
    val mp: RDD[people] = spalited.map(a => {
      people(a(0).toInt, a(1), a(2).toInt)
    })
    import sqlContext.implicits._
    val df: DataFrame = mp.toDF()
    df.show()
  }

}

case class people(id: Int, name: String, age: Int)