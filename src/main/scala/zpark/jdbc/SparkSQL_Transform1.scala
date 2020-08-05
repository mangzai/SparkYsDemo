package zpark.jdbc

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @author ys
 * @date 2020/5/4  10:31
 */
object SparkSQL_Transform1 {
  def main(args:Array[String]):Unit={
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_Transform1")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    //创建Rdd
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.parallelize(List((1, "zhangsan", 12), (2, "lisi", 33)))
    //RDD-DateSet
    val userRDD: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val userDS: Dataset[User] = userRDD.toDS()
    val rdd1: RDD[User] = userDS.rdd
    rdd1.foreach(println)
    //释放资源
    spark.stop()
  }

}
