package zpark.jdbc

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.JdbcRDD

object SparkMysql {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = initSparkContext
    val driver="com.mysql.jdbc.Driver"
    val url="jdbc:mysql://localhost:3306/db_spark"
    val user="root"
    val password="rootroot"

    val sql="select name,age from tb_person where id>=? and id <=?"
    val jdbcRDD = new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, user, password)
      },
      sql,
      1,
      100,
      1,
      (rs) => {
        println(rs.getString(1) + "," + rs.getInt(2))
      }
    )
    jdbcRDD.collect()


    sc.stop()
  }

  private def initSparkContext = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val sc = new SparkContext(conf)
    sc

  }

}
