package zpark.kafka

import java.util
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

/**
 * @author ys
 *         data 2020/6/24 10:23
 */
object KafkaConsumerDemo {
  def main(args: Array[String]): Unit = {
    //配置信息
    val props = new Properties()
    props.put("bootstrap.servers", "hdp-101:9092")
    //消费者组
    props.put("group.id", "group01")
    //指定消费位置:earliest/latest/none
    props.put("auto.offset.reset", "earliest")
    //指定消费者的key反序列化
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //指定消费者的value反序列化
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("enable.auto.commit", "true")
    props.put("session.timeout.ms", "30000")
    //得到实例
    val kafkaConsumer = new KafkaConsumer[String, String](props)
    //首先订阅topic
    kafkaConsumer.subscribe(Collections.singletonList("ys"))
    //开始消费数据
    while (true) {
      // 如果Kafak中没有消息，会隔timeout这个值读一次。比如上面代码设置了2秒，也是就2秒后会查一次。
      // 如果Kafka中还有消息没有消费的话，会马上去读，而不需要等待。
      val msgs: ConsumerRecords[String, String] = kafkaConsumer.poll(2000)
    //  println(msgs.count())
      val it: util.Iterator[ConsumerRecord[String, String]] = msgs.iterator()
      while (it.hasNext) {
        val msg: ConsumerRecord[String, String] = it.next()
        println(s"partition: ${msg.partition()}, offset: ${msg.offset()}, key: ${msg.key()}, value: ${msg.value()}")

      }
    }
  }

}
