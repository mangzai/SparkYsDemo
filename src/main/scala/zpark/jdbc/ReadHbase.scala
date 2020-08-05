package zpark.jdbc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 * @date 2020/4/20  21:54
 */
object ReadHbase {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkHbase")
    val sc: SparkContext = new SparkContext(conf)

    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "hdp-101,hdp-102,hdp-103")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181") //zookeeper端口
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "student") //扫描哪张表
    /*参数1：hbase的配置信息
      参数2：classOf[TableInputFormat] 读数据的数据类型
      参数3/4：calssOf[ImmutableBytesWritable],classOf[Result]
      读到的数据的rdd元祖中类型
    */


    val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]
    )
    val count: Long = rdd.count()
    print(count)
    rdd.cache()
    rdd.foreach({ case (_, result) =>
      val key: String = Bytes.toString(result.getRow)
      val value: String = Bytes.toString(result.value())
      println(key,value)

      //val name: String = Bytes.toString(result.getValue("lie1".getBytes, "name".getBytes))
      //println("ROW:" + key + "name:" + name )
      //val age: String = Bytes.toString(result.getValue("lie2".getBytes, "age".getBytes))
   /*   val name: Array[Byte] = result.getValue("info1".getBytes, "name".getBytes())
      val sex: Array[Byte] = result.getValue("info1".getBytes, "sex".getBytes())
      val age: Array[Byte] = result.getValue("info1".getBytes, "age".getBytes())*/
     // val name: String = Bytes.toString(result.getValue("lie1".getBytes, "name".getBytes))
      //val age: String = Bytes.toString(result.getValue("lie2".getBytes, "age".getBytes))
      //println("ROW:" + key + "name:" + name + "Age:" + age)
     /* println("ROW:"+key)
      println("info:"+"name:"+name+"sex"+sex+"age"+age)*/
    })

  }
}
