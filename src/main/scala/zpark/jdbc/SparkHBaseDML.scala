package zpark.jdbc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Table}

/**
 * @author ys
 * @date 2020/4/23  11:03
 */
object SparkHBaseDML {
  //创建hbase配置文件
  private val configuration: Configuration = HBaseConfiguration.create()
  configuration.set("hbase.zookeeper.quorum", "hdp-101,hdp-102,hdp-103")
  configuration.set("hbase.zookeeper.property.clientPort", "2181") //zookeeper端口
  // 建立连接
  private val connection: Connection = ConnectionFactory.createConnection(configuration)
  //获取管理员
  private val admin: Admin = connection.getAdmin
  //创建表名称对象通过这个对象才可以操作
  private val tableName: TableName = TableName.valueOf("student")
  //通过连接对象，操作表中数据
  private val table: Table = connection.getTable(tableName)
}
