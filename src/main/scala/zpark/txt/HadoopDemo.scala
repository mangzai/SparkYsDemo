package zpark.txt

import java.io.StringWriter

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 * @date 2020/4/15  9:35
 */
object HadoopDemo {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = initSpark
    val inputFile: String = "in/people.json"
    val outputFile: String = "out/hdfsout3/hdfsfile.json"
    val job=new Job()
    val data: RDD[(Text, Text)] = sc.newAPIHadoopFile(
      inputFile,
      classOf[KeyValueTextInputFormat],
      classOf[Text],
      classOf[Text],
      job.getConfiguration
    )
    data.foreach(println)

    data.saveAsNewAPIHadoopFile(
      outputFile,
      classOf[Text],
      classOf[Text],
      //classOf[TextOutputFormat[Text,Text]],
      //classOf[TextOutputFormat[StringWriter,StringWriter]],
      classOf[TextOutputFormat[Text,Text]],
      job.getConfiguration
    )
    //data.saveAsNewAPIHadoopFile(outputFile,classOf[Text],classOf[Text],classOf[TextOutputFormat[Text,Text]],job.getConfiguration)

  }

  private def initSpark = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HDfs")
    val sc = new SparkContext(conf)
    sc
  }
}
