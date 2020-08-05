package zpark.data

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 * @date 2020/4/14  9:56
 */
object SparkboradDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)
    val rdd1: RDD[(Int, String)] = sc.parallelize(List((1, "a"), (2, "b"), (3, "c")))
    val list = List((1, 1), (2, 2), (3, 3))
    val broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)
    rdd1.map{
      case (key,value)=>{
        var v2:Any=null
        for(t<-broadcast.value){
          if(key==t._1){
            v2=t._2
          }
        }
      }
    }
  }

}
