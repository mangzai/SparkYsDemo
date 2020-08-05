package zpark.jdbc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 * @date 2020/4/23  9:03
 */
object SparkHBase {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("HBase"))
    val configuration: Configuration = HBaseConfiguration.create()
    configuration.set(TableInputFormat.INPUT_TABLE, "student") //扫描哪张表
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      configuration,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    hbaseRDD.foreach{
      case(rowKey,result)=>{
        val cells: Array[Cell] = result.rawCells()
        for(cell <-cells){
          println(Bytes.toString((CellUtil.cloneValue(cell))))
        }
      }
    }
    sc.stop()
  }

}
