package zpark.sparkTools

import org.apache.spark.{SparkConf, SparkContext}

object SparkPVUV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("E:\\IdeaProject\\sparkDemo\\src\\main\\scala\\com\\zpark\\sparkTools\\data\\pvdc.txt")
    //pv
    lines.map(line=>{(line.split("\t")(5),1)}).reduceByKey((v1:Int,v2:Int)=>{
      v1+v2
    }).sortBy(tp=>{tp._2},false).foreach(println)
    //uv
    lines.map(line=>{line.split("\t")(0)+"_"+line.split("\t")(5)})
      .distinct()
      .map(one=>{(one.split("_")(1),1)})
      .reduceByKey(_+_)
      .sortBy(_._2,false)
      .foreach(println)
  }
}
