package zpark.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 * @date 2020/4/29  17:41
 */
object CacheDemo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CacheDemo1")
    val sc = new SparkContext(conf)
    var rdd: RDD[String] = sc.textFile("in/pvdc.txt")
    //rdd = rdd.cache()
    rdd.persist(StorageLevel.MEMORY_ONLY_SER)
    val startTime1: Long = System.currentTimeMillis()
    val result1: Long = rdd.count()
    val endTime1: Long = System.currentTimeMillis()
    println(s"first条数=$result1,time=${endTime1-startTime1}")


    val startTime2: Long = System.currentTimeMillis()
    val result2: Long = rdd.count()
    val endTime2: Long = System.currentTimeMillis()
    println(s"second条数=$result2,time=${endTime2-startTime2}")

    rdd.count()
    sc.stop()
  }

}
