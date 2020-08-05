package zpark.mlib

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 *         data 2020/7/6 10:38
 */
object TestMilb1 {
  val spamFile = "in/spam.txt"
  val hamFlie = "in/ham.txt"

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ETL")
    val sc = new SparkContext(conf)
    val spam: RDD[String] = sc.textFile(spamFile)
    val ham: RDD[String] = sc.textFile(hamFlie)
    val tf = new HashingTF(10000)
    //得到特征向量
    val spamTrans: RDD[linalg.Vector] = spam.map(line => tf.transform(line.split(" ")))
    val hamTrans: RDD[linalg.Vector] = ham.map(line => tf.transform(line.split(" ")))
    //存放邮件lebeledPoint
    val spamLab: RDD[LabeledPoint] = spamTrans.map(m => LabeledPoint(0, m))
    val hamLab: RDD[LabeledPoint] = hamTrans.map(n => LabeledPoint(1, n))
    //得到训练数据
    val tainData: RDD[LabeledPoint] = spamLab.union(hamLab)
    //保存起来
    tainData.persist()
    //创建回归模型
    val logistSGD: LogisticRegressionWithSGD = new LogisticRegressionWithSGD()
    val model: LogisticRegressionModel = logistSGD.run(tainData)
    val infoVec1: linalg.Vector = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
    val info1: Double = model.predict(infoVec1)
    println(info1)

    val info2Vec: linalg.Vector = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))
    val info2: Double = model.predict(info2Vec)
    println(info2)
    sc.stop()
  }

}
