package zpark.sparkDemo

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import sun.rmi.log.ReliableLog.LogFile

object PartitionDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("partitionDemo")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val userdataFile:String="E:\\IdeaProject\\sparkDemo\\src\\main\\scala\\com\\zpark\\a.txt"
    val userdataTmp: RDD[String] = sc.textFile(userdataFile)
    val userDataNo: RDD[(String, String)] = userdataTmp.map(x => (x.split(",")(0), x.split(",")(1)))
    val userData = userDataNo .partitionBy(new HashPartitioner(4)).persist()

    val eventFile:String="E:\\IdeaProject\\sparkDemo\\src\\main\\scala\\com\\zpark\\b.txt"
    val eventTmp: RDD[String] = sc.textFile(eventFile)
    val event: RDD[(String, String)] = eventTmp.map(x => (x.split(",")(0), x.split(",")(1)))
//++++++++++++++++++++++++

    val filted: _root_.org.apache.spark.rdd.RDD[(_root_.scala.Predef.String, (_root_.scala.Predef.String, _root_.scala.Predef.String))] = processNewLog(userData, event)

    //++++++++++++++++++
    var count =1
    while (count<5){
      count+=1
      processNewLog(userData,event)
      Thread.sleep(1000)
    }
    println(filted.count())
  }

  private def processNewLog(userData: RDD[(String, String)], event: RDD[(String, String)]) = {
    val joined: RDD[(String, (String, String))] = userData.join(event)
    val filted = joined.filter {
      case (userId, (userInfo, linkInfo)) => !userInfo.contains(linkInfo)
    }
    filted
  }
}
