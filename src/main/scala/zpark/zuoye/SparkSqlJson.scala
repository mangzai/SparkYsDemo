package zpark.zuoye

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
 * @author ys
 *         data 2020/7/2 9:50
 */
object SparkSqlJson {
  val inputPath = "in/index.json"

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ETL")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sqlContext: SQLContext = sparkSession.sqlContext
    val frame: DataFrame = sqlContext.read.format("json").load(inputPath)
     import org.apache.spark.sql.functions.explode
    val df: DataFrame = frame.select(explode(frame("result.data"))).toDF("mdata")
    df.printSchema()
    df.select("mdata.category").show(50)
  }
}
