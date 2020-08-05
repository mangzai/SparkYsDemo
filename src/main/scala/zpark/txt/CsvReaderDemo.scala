package zpark.txt

import java.io.StringReader

import com.opencsv.CSVReader
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 * @date 2020/4/13  20:24
 */
object CsvReaderDemo {
  def main(args: Array[String]): Unit = {
  /*  val stringReader = new StringReader("")
    val reader: CSVReader = new CSVReader(stringReader)
    reader.readNext()*/
    //读的方法已经有了，需要把这个方法用到我们的spark程序中，rdd
    val sc: SparkContext = initSpark
    val lines: RDD[String] = sc.textFile("in/favourite_animals.csv")
    val result: RDD[Array[String]] = lines.map(lines => {
      val stringReader = new StringReader(lines)
      val redear = new CSVReader(stringReader)
      redear.readNext()
    })
    result.foreach(x=>{
      x.foreach(println)
      println("+++++++++++++++++")
    })
  }

  private def initSpark = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CsvDemo")
    val sc = new SparkContext(conf)
    sc
  }
}
