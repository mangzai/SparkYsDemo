package zpark.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object SparkMysql1 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = initSparkContext

    val driver="com.mysql.jdbc.Driver"
    val url="jdbc:mysql://localhost:3306/db_spark"
    var user="root"
    val password="rootroot"

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("zhangsan", 19), ("lisi", 20)),2)
   /* dataRDD.foreach{
      case (username,age)=>{
        Class.forName(driver)
        val connection: Connection = DriverManager.getConnection(url, user, password)
        val sql ="insert into tb_person (name, age) values(?,?)"
        val statement: PreparedStatement = connection.prepareStatement(sql)
        statement.setString(1,username)
        statement.setInt(2,age)
        statement.executeUpdate()
        statement.close()
        connection.close()
      }
    }*/
    dataRDD.foreachPartition(datas=>{
      Class.forName(driver)
      val connection: Connection = java.sql.DriverManager.getConnection(url, user, password)
      datas.foreach{
        case (username,age)=>{
          val sql ="insert into tb_person (name, age) values(?,?)"
          val statement: PreparedStatement = connection.prepareStatement(sql)
          statement.setString(1,username)
          statement.setInt(2,age)
          statement.executeUpdate()
        }
      }
      connection.close()
    })

    sc.stop()
  }

  private def initSparkContext = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("save")
    val sc = new SparkContext(conf)
    sc
  }
}
