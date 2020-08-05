package zpark.dstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author ys
 * @date 2020/5/5  11:03
 */
object SparkStreaming02_FileDataSource {
  def main(args: Array[String]): Unit = {
    //spark配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("File")
    //实时环境对象
    val steamingContext = new StreamingContext(conf, Seconds(5))

    val fileDStream: DStream[String] = steamingContext.textFileStream("StreamSourceFile")
    //将采集的数据进行分解（扁平化）
    val wordDStream: DStream[String] = fileDStream.flatMap(line => line.split(" "))
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))
    //将转换结构后的数据进行聚合处理
    val wordToSumStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)
    //将结果打印
    wordToSumStream.print()
    //启动采集器
    steamingContext.start()
    //Driver等待采集器的执行
    steamingContext.awaitTermination()
  }

}
