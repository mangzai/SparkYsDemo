package zpark.dstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author ys
 * @date 2020/5/6  12:22
 */
/*自定义采集器
* */
//window操作
object SparkStreaming06_Window {
  def main(args: Array[String]): Unit = {
    //scala中的窗口
    /*val ints = List(1, 2, 3, 4, 5, 6)
    val intses: Iterator[List[Int]] = ints.sliding(3, 3)
    for (list <- intses) {
      println(list.mkString(","))
    }*/

    //使用sparkStreaming的窗口
    //spark配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sliding")
    val streamingContext = new StreamingContext(conf, Seconds(3))
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "hdp-101:2181",
      "test-consumer-group",
      Map("atguigu" -> 3)
    )
    //窗口大小应该是采集周期的整数倍,窗口滑动步长也应该是采集周期的整数倍
    val windoDStream: DStream[(String, String)] = kafkaDStream.window(Seconds(9), Seconds(3))
    val wordDStream: DStream[String] = windoDStream.flatMap(t => t._2.split(" "))
    //将数据进行结构的转换方便统计
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))
    //将转换结构后的数据进行聚合操作
    val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)
    wordToSumDStream.print()
    //启动采集器
    streamingContext.start()
    //Driver等待采集器的执行
    streamingContext.awaitTermination()

  }

}
