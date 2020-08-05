package zpark.zuoye

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
 * @author ys
 * data 2020/6/16 13:22         22222222222222222222222
 */
object SparkSqlZuoye {
   val localPath = "in/zuoye.json"

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ETL")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sqlContext: SQLContext = sparkSession.sqlContext
    val sc: SparkContext = sparkSession.sparkContext
    val df: DataFrame = sparkSession.read.format("json").load(localPath)
    df.createOrReplaceTempView("user")
    //年齡大于18岁的所有数据
    val sql1="select * from user"
    //去重
    val sql2="select avg(age) from (select distinct name,age,id from user)"
    val sql3="select distinct name ,age, id from user"
    val sql4="select name as username from user"
    val sql5="select distinct name,age from user "
    val frame: DataFrame = sqlContext.sql(sql5)
    val url="jdbc:mysql://localhost:3306/db_spark?charactorEncoding=utf-8&serverTimezone=UTC"
    val table="person2"
    val props=new Properties()
    props.put("user","root")
    props.put("password","rootroot")
    props.put("driver","com.mysql.jdbc.Driver")
    frame.write.mode("append").jdbc(url,table,props)

    frame.show()
  }

}
