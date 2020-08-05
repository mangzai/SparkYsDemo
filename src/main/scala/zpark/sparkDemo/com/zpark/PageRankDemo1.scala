package zpark.sparkDemo.com.zpark

import java.net.URL

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD




object PageRankDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("PageRankDemo1")
    val sc = new SparkContext(conf)
    val linkstemp: RDD[(String, List[String])] = sc.parallelize(
      List(
        ("http://www.baidu.com/aaa",List("http://www.sina.com","http://www.youdao.com")),
        ("http://www.sina.com",List("http://www.baidu.com/aaa","http://www.youdao.com")),
        ("http://www.youdao.com",List("http://www.baidu.com/aaa","http://www.sina.com","http://www.jd.com")),
        ("http://www.jd.com",List("http://www.youdao.com"))
      )
    )
    val links: RDD[(String, List[String])] = linkstemp.partitionBy(new DomainNamePartitioner(6)).persist()
    var ranks: RDD[(String, Double)] = links.mapValues(v => 1.0)

    for (i <- 0 until 10) {
      val joined: RDD[(String, (List[String], Double))] = links.join(ranks)
      val contributions: RDD[(String, Double)] = joined.flatMap {
        case (pageId, (linkss, rank)) => linkss.map(x => (x, rank / linkss.size))
      }
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
    }
    ranks.foreach(println(_))
  }

}

class DomainNamePartitioner(numParts:Int) extends Partitioner {
  def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    //取网址getHost
    val domain: String = new URL(key.toString).getHost
    val code: Int = domain.hashCode % numPartitions
    if(code<0) {
      code+numPartitions
    } else {
      code
    }
  }

  override def equals(other: Any): Boolean = other match {
    case dnp:DomainNamePartitioner =>
      dnp.numPartitions == numPartitions
    case _=>
      false
  }
}
