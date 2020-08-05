package zpark.structure

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 * @date 2020/4/22  10:54
 */
object Broad {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Borad"))
    broadcastJoin(sc)
    Thread.sleep(30000 * 10)
    sc.stop()

  }

  def broadcastJoin(sc: SparkContext): Unit = {
    //小数据 ->广播
    val info1: collection.Map[String, String] = sc.parallelize(Array(("101", "小明"), ("102", "小红"))).collectAsMap()
    //Driver数据才需要广播
    val broadcastinfo1: Broadcast[collection.Map[String, String]] = sc.broadcast(info1)

    //大数据
    val info2: RDD[(String, (String, String))] = sc.parallelize(Array(("101", "科技", "120"), ("102", "金融", "125"), ("103", "军事", "130")))
      .map(x => (x._1, (x._2, x._3)))
    //broadcst以后就不用join实现，而是大表数据读取出来一条就和广播出去的小表记录做匹配
    info2.mapPartitions(x => {
      val boradcastMap: collection.Map[String, String] = broadcastinfo1.value
      for ((key, value) <- x if boradcastMap.contains(key))
        yield (key, boradcastMap.get(key).getOrElse(), value._1)
    }).foreach(println)
  }


}
