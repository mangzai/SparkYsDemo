package zpark.data

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 * @date 2020/4/14  9:02
 */
object SparkDataAdd {
  def main(args:Array[String]):Unit={
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.parallelize(List(2, 3, 4, 5), 2)
    //使用累加器共享变量
    //创建累加器对象
    val accumulator: LongAccumulator = sc.longAccumulator
    var sum: Int = 0
    dataRDD.foreach{
      case i=>{
        accumulator.add(i)
      }
    }
    println("sum="+ accumulator.value)
    //释放资源
    sc.stop()

  }

}
