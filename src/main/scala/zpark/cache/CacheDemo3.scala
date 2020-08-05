package zpark.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 * @date 2020/4/29  17:41
 */
object CacheDemo3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CacheDemo1")
    val sc = new SparkContext(conf)
    var rdd: RDD[String] = sc.textFile("in/pvdc.txt")
    sc.setCheckpointDir("in/ck")
    rdd.checkpoint()
    rdd.count()
    sc.stop()
  }

}
