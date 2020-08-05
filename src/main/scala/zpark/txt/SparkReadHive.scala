package zpark.txt

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author ys
 * @date 2020/4/16  8:36
 */
object SparkReadHive {
  def main(args: Array[String]): Unit = {
    val hiveFilePath: String = "in/people.json"
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkReadHive")
    val sc: SparkContext = new SparkContext(conf)
    val hc: HiveContext = new HiveContext(sc)
    val frame: DataFrame = hc.read.json(hiveFilePath)
    frame.createTempView("people")
//    frame.createOrReplaceGlobalTempView("people")
    val result: DataFrame = hc.sql("select name from people")
    println(result.first())


  }

}
