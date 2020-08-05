package zpark.structure

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** 广播变量是调优的策略
 *
 * @author ys
 * @date 2020/4/24  8:40
 */
object SparkBroad {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("broad"))
    val list = List((1, 2), (2, 3), (3, 4))
    //可以使用广播变量减少数据的传输
    //1.构建广播变量
    val broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)
    val rdd1: RDD[(Int, String)] = sc.parallelize(List((1, "xiaoming"), (2, "xiaohong")))
    val resultRDD: RDD[(Int, (String, Any))] = rdd1.map {
      case (key, value) => {
        var v2: Any = null
        for (t <- broadcast.value) {
          if (key == t._1) {
            v2 = t._2
          }
        }
        (key, (value, v2))
      }
    }
    resultRDD.foreach(println)
  }

}
