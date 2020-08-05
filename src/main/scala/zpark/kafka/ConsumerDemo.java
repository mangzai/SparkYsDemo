package zpark.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @author ys
 * data 2020/6/24 10:47
 */
public class ConsumerDemo {
    private static KafkaConsumer<String, String> consumer;
    private static Properties props;

    static {
        props = new Properties();
        //消费者kafkka地址
        props.put("bootstrap.servers", "hdp-102 :9092");
        //key反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //组
        props.put("group.id", "yangk");
    }

    /**
     * 从kafka中获取数据（SpringBoot也集成了kafka）
     */
    private static void ConsumerMessage() {
        //允许自动提交位移
        props.put("enable.auto.commit", true);
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singleton("animal"));

        //使用轮询拉取数据--消费完成之后会根据设置时长来清除消息，被消费过的消息，如果想再次被消费，可以根据偏移量(offset)来获取
        try {
            while (true) {
                //从kafka中读到了数据放在records中
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> r : records) {
                    System.out.printf("topic = %s, offset = %s, key = %s, value = %s", r.topic(), r.offset(),
                            r.key(), r.value());

                }
            }
        } finally {
            consumer.close();
        }
    }

    public static void main(String[] args) {
        ConsumerMessage();
    }

}
