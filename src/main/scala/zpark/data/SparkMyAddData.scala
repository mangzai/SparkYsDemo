package zpark.data

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 * @date 2020/4/14  8:52
 */
object SparkMyAddData {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)
    val dataRDD: RDD[String] = sc.makeRDD(List("hadoop","hive","hbase","Scala","Spark"),2 )
    //val i: Int = dataRDD.reduce((x: Int, y: Int) => x + y)
    //println(i)
    //创建累加对象
    //val accumulator: LongAccumulator = sc.longAccumulator

   //TODO  创建累加器
    val wordAccumulator: WordAccoumulator = new WordAccoumulator
    //TODO 注册累加器
    sc.register(wordAccumulator)

    dataRDD.foreach{
      case word=>{
        //TODO 执行累加器累加功能
        wordAccumulator.add(word)
      }
    }
    //TODO 获取累加器的值
    println("sum="+ wordAccumulator.value)
  }
}
//声明累加器
//1.继承Accounmulator
//实现抽象方法
class WordAccoumulator extends AccumulatorV2[String, util.ArrayList[String]]  {
  val list=new util.ArrayList[String]()
  //当前的累加器是否为初始化状态
  override def isZero: Boolean = list.isEmpty
//复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccoumulator
  }
//重置累加器对象
  override def reset(): Unit = {
    list.clear()
  }
//向累加器中增加数据
  override def add(v: String): Unit = {
    if(v.contains("h")){
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