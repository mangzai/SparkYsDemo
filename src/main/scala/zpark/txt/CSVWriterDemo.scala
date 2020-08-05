package zpark.txt

import java.io.{StringReader, StringWriter}

import com.opencsv.{CSVReader, CSVWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

/**
 * @author ys
 * @date 2020/4/13  21:49
 */
object CSVWriterDemo {
  def main(args: Array[String]): Unit = {
    /*val stringReader = new StringReader("")
    val reader: CSVReader = new CSVReader(stringReader)
    reader.readNext()*/
    ////////////
    val sc: SparkContext = initSpark
    val lines: RDD[String] = sc.textFile("in/favourite_animals.csv")
    val result: RDD[Array[String]] = lines.map(lines => {
      val stringReader = new StringReader(lines)
      val reader: CSVReader = new CSVReader(stringReader)
      reader.readNext()
    })
    result.foreach(x =>{
      x.foreach(println)
      println("=================")
    })
    val stringWriter: StringWriter = new StringWriter()
    val writer: CSVWriter = new CSVWriter(stringWriter)
    val array: Array[String] = Array("hello", "clown")
    val list: List[Array[String]] = List(array)
    writer.writeAll(list)
    println(stringWriter.toString)
    val input: RDD[Array[String]] = sc.parallelize(list)

    val res: RDD[String] = input.mapPartitions(arr => {
      val stringWriters: StringWriter = new StringWriter()
      val writers: CSVWriter = new CSVWriter(stringWriters)

      val list2: List[Array[String]] = arr.toList

      //      val list1: List[String] = list2
      //      val toArray: Array[String] = arr.toArray
      //      writers.writeAll(list1)
      writers.writeAll(list2)
      Iterator (stringWriters.toString)
    })
    res.saveAsTextFile("output/")
  }


  def initSpark:SparkContext = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("CsvDemo")
    val sc: SparkContext = new SparkContext(conf)
    sc
  }

}
