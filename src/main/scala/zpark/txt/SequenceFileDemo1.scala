package zpark.txt

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 * @date 2020/4/15  8:39
 */
object SequenceFileDemo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Sequence")
    val sc = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("pande", 3), ("key", 6), ("snail", 3)))
    rdd.saveAsSequenceFile("out/seq")

    val lines: RDD[(Text, IntWritable)] = sc.sequenceFile("out/seq", classOf[Text], classOf[IntWritable])
    lines.map{
      case(x,y)=>()
    }
  }

}
