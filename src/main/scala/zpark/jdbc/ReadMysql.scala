package zpark.jdbc

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ys
 * @date 2020/4/20  21:12
 */
object ReadMysql {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkhive")
    val sc = new SparkContext(conf)
    val data: JdbcRDD[(Int, String)] = new JdbcRDD(sc,
      createConnection,
      "select * from tb_person where ? <id and id <=?",
       0,
      10,
       2,
      mapRow = extractValues
    )
    println(data.collect().toList)
  }
def createConnection() ={
  Class.forName("com.mysql.jdbc.Driver").newInstance()
  DriverManager.getConnection("jdbc:mysql://localhost:3306/db_spark?user=root&password=rootroot&serverTimezone=GMT%2B8")
}

  def extractValues(r:ResultSet)={
    (r.getInt(1),r.getString(2))
  }

}
