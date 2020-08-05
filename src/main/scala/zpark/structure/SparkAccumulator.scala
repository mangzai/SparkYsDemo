package zpark.structure

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/** 自定义累加器
 *
 * @author ys
 * @date 2020/4/24  8:16
 */
object SparkAccumulator {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("accumulator"))
    val dataRDD: RDD[String] = sc.makeRDD(List("hbase", "hive", "hadoop", "spark", "storm", "sqoop", "kafaka", "scala", "zk"), 2)
    //TODO 创建累加器
    val wordAccumulator = new WordAccumulator
    //todo 注册累加器
    sc.register(wordAccumulator)
    dataRDD.foreach {
      case word => {
        //todo 执行累加器功能
        wordAccumulator.add(word)
      }
    }
    //todo 获取累加器的值
    println("sum=" + wordAccumulator.value)
    sc.stop()

  }

  //声明累加器
  //继承Accumulator2
  //实现抽象方法
  //创建累加器
  class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {

    val list = new util.ArrayList[String]()

    //当前累加器是否为初始化对象
    override def isZero: Boolean = {
      list.isEmpty
    }

    //复制累加器对象
    override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
      new WordAccumulator()
    }

    //重置累加器对象
    override def reset(): Unit = {
      list.clear()
    }

    //向累加器中增加数据
    override def add(v: String): Unit = {
      if (v.contains("h")) {
        list.add(v)
      }
    }

    //合并
    override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
      list.addAll(other.value)
    }

    //获取累加器的结果
    override def value: util.ArrayList[String] = list
  }

}