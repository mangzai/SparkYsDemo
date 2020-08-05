package zpark.structure

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/** 累加器
 *
 * @author ys
 * @date 2020/4/24  8:01
 */
object SparkShareData {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("shareData").setMaster("local[*]"))
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 3, 4, 5, 6), 2)
    /*    val i: Int = dataRDD.reduce(_ + _)
        println(i)*/
    /* var sum =0
     dataRDD.foreach(i=>sum=sum+i)
     println("sum="+sum)
 */
    //使用累加器来共享变量，来累加数据
    var sum = 0
    val accumulator: LongAccumulator = sc.longAccumulator
    dataRDD.foreach {
      case i => {
        //执行累加操作
        accumulator.add(i)
      }
    }
    println("sum=" + accumulator.value)
    sc.stop()
  }

}
