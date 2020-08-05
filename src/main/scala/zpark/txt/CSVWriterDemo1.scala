package zpark.txt

import java.io.StringWriter

import com.opencsv.{CSVReader, CSVWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
 * @author ys
 * @date 2020/4/13  22:20
 */
object CSVWriterDemo1 {
  def main(args: Array[String]): Unit = {
    //val stringWriter: StringWriter = new StringWriter()
    //val writer = new CSVWriter(stringWriter)
    val array: Array[String] = Array("hello", "hi")
    val list: List[Array[String]] = List(array)
    //writer.writeAll(list)
    //println(stringWriter.toString)
    val sc: SparkContext = initSpark
    val input: RDD[Array[String]] = sc.parallelize(list)
    val res: RDD[String] = input.mapPartitions(arr => {
      val stringWriters: StringWriter = new StringWriter()
      val writers: CSVWriter = new CSVWriter(stringWriters)
      val list2: List[Array[String]] = arr.toList
      val list1: List[Array[String]] = list2
      writers.writeAll(list2)
     // writers.writeAll(list1)
      Iterator(stringWriters.toString)
    })
    res.saveAsTextFile("out/csv2")
  }

  private def initSpark = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CSVWriter1")
    val sc = new SparkContext(conf)
    sc
  }
}
