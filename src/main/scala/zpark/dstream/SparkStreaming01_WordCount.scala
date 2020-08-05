package zpark.dstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author ys
 * @date 2020/5/5  10:09           33333333333333333333333333333333
 */
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //使用SparkStreaming完成WordCount
    //spark配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
    //实时数据分析的环境对象
    val streamingContext = new StreamingContext(conf, Seconds(3))
    //从指定的端口中采集数据
    val socketLineDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hdp-101", 9999)
    //将采集的数据进行分解（扁平化）
    val wordDStream: DStream[String] = socketLineDStream.flatMap(line => line.split(" "))
    //将数据进行结构的转换方便统计分析
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))
    //将转换结构后的数据进行聚合处理
    val wordToSumDSteam: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)
    //将结果打印出来
    wordToSumDSteam.print()
    //不能停止采集程序所以不能使用streamingContext.stop()


    //启动采集器
    streamingContext.start()
    //Drvier等待采集器执行
    streamingContext.awaitTermination()
  }

}
