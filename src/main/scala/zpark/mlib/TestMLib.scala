package zpark.mlib

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author ys
 *         data 2020/7/6 10:27
 */
object TestMLib {
  val spamFile = "in/spam.txt"
  val normalFile = "in/ham.txt"
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("testMlit")
    val sc = new SparkContext(conf)
    val spam: RDD[String] = sc.textFile(spamFile)
    val normal: RDD[String] = sc.textFile(normalFile)

    //
    val tf = new HashingTF(10000)
    //这是spam（垃圾邮件）的特征向量
    val spamVec: RDD[linalg.Vector] = spam.map(line => tf.transform(line.split(" ")))
    //这是正常邮件的特征向量
    val normalVec: RDD[linalg.Vector] = normal.map(line => tf.transform(line.split(" ")))

    //这是存放垃圾邮件的LabeledPoint
    val spamLab: RDD[LabeledPoint] = spamVec.map(features => LabeledPoint(1,features))
    //
    val normalLab: RDD[LabeledPoint] = normalVec.map(features => LabeledPoint(0,features))

    //得到训练数据
    val trainingData: RDD[LabeledPoint] = spamLab.union(normalLab)
    trainingData.persist()



    //得到一个模型（逻辑回归）
    val logisticRWS: LogisticRegressionWithSGD = new LogisticRegressionWithSGD()
    val model: LogisticRegressionModel = logisticRWS.run(trainingData)



    val infoVec1: linalg.Vector = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
    val info1: Double = model.predict(infoVec1)
    println(info1)

    val info2Vec: linalg.Vector = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))
    val info2: Double = model.predict(info2Vec)
    println(info2)
    //    // 垃圾邮件测试
    //    println(model.predict(tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))))
    //    // 正常邮件测试
    //    println(model.predict(tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))))


    sc.stop()

  }
}
