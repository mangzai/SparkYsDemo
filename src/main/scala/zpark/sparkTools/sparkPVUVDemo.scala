package zpark.sparkTools

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object sparkPVUVDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("E:\\IdeaProject\\sparkDemo\\src\\main\\scala\\com\\zpark\\sparkTools\\data\\pvdc.txt")
    //每个网址的每个地区访问量，由大到小排序
    val site_local = lines.map(line => {(line.split("\t")(5), line.split("\t")(1))})
    val site_localTerable = site_local.groupByKey()
    val result= site_localTerable.map(one => {
      val localMap = mutable.Map[String, Int]()
      val site = one._1
      val localIter = one._2.iterator
      while (localIter.hasNext) {
        val local = localIter.next()
        if (localMap.contains(local)) {
          val value = localMap.get(local).get+1
          localMap.put(local, value)
        } else {
          localMap.put(local, 1)
        }
      }
      //对map排序
      val tuples: List[(String, Int)] = localMap.toList.sortBy(one => {
        one._2
      })
      if(tuples.size>3){
        val returnList=new ListBuffer[(String,Int)]()
        for(i <-0 to 2){
          returnList.append(tuples(i))
        }
        (site, returnList)
      }else{
        (site, tuples)
      }

    })
    result.foreach(println)
    result.saveAsTextFile("E:\\IdeaProject\\sparkDemo\\src\\main\\scala\\com\\zpark\\sparkTools\\data\\pvdc1")
  }

}
