package zpark.sparkDemo.com.zpark

import java.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PageRankDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("PageRankDemo")
    val sc: SparkContext = new SparkContext(conf)
    val linkstemp: RDD[(String, List[String])] = sc.parallelize(
      List(
        ("A", List("B", "C")),
        ("B", List("A", "C")),
        ("C", List("A", "B", "D")),
        ("D", List("C"))
      )
    )
    val links: RDD[(String, List[String])] = linkstemp.partitionBy(new HashPartitioner(100)).persist()
    var ranks: RDD[(String, Double)] = links.mapValues(v => 1.0)
//遍历10次就是模仿浏览
    for(i<-0 until 10){
      val joined: RDD[(String, (List[String], Double))] = links.join(ranks)
      val contributions: RDD[(String, Double)] = joined.flatMap {
        case (pageId, (linkss, rank)) => linkss.map(x => (x, rank / linkss.size))
      }
    ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
    }
    ranks.foreach(println(_))
  }

}

