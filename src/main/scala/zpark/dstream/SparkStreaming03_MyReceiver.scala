package zpark.dstream

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author ys
 * @date 2020/5/6  12:22
 */
/*自定义采集器
* */
object SparkStreaming03_MyReceiver {
  def main(args: Array[String]): Unit = {
    //spark环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Wordcount")
    //实时环境对象
    val streamingContext = new StreamingContext(conf, Seconds(3))
    //采集数据
    val receiverDSteam: ReceiverInputDStream[String] = streamingContext.receiverStream(new MyReceiver("hdp-101", 999))
    //将采集到的数据扁平化
    val wordDStream: DStream[String] = receiverDSteam.flatMap(line => line.split(" "))
    //将单词转换成pairRDD
    val mapDStream: DStream[(String, Int)] = wordDStream.map(word => (word, 1))
    //统计相同单词的个数
    val count: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)
    count.print()
    streamingContext.start()
    streamingContext.awaitTermination()


  }

}
//声明采集器
class  MyReceiver(host:String ,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  var socket:Socket = null
  def receive() :Unit={
    socket=new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))
    var line:String=null
    while ((line=reader.readLine())!=null){
      //将采集数据存储到采集器的内部进行转换
      if("END".equals(line)){
        return

      }else{
        this.store(line)
      }
    }
  }

  override def onStart(): Unit = {
  new Thread(new Runnable {
    override def run(): Unit = {
      receive()
    }
  }).start()
  }

  override def onStop(): Unit = {
    if(socket!=null){
      socket.close()
      socket=null
    }
  }
}