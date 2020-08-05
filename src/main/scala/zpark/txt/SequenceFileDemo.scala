package zpark.txt

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 * @date 2020/4/15  8:46
 */
object SequenceFileDemo {

  def initSpark(): SparkContext = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SequenceFileDemo")
    val sc = new SparkContext(conf)
    sc
  }

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = initSpark()
  /*  val nameData: RDD[(String, Int)] = sc.makeRDD(List(("ys", 2), ("csq", 5), ("yp", 7), ("yx", 6)))
    nameData.saveAsSequenceFile("out/seq1")*/

    val lines: RDD[(Text, IntWritable)] = sc.sequenceFile("out/seq1", classOf[Text], classOf[IntWritable])
    val output: RDD[(String, Int)] = lines.map {
      case (x, y) => (x.toString, y.get())
    }
    output.foreach(println)


    sc.stop()
  }

}
