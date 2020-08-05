package zpark.jdbc

import java.util


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ArrayBuffer

/**
 * @author ys
 * @date 2020/4/22  10:14
 */
object ReadHbase1 {

  // 创建hbase配置文件
  val con: Configuration = HBaseConfiguration.create()
  // 设置zookeeperjiqun
//  con.set("hbase.zookeeper.quorum","node2:2181,node3:2181,node4:2181")
  con.set("hbase.zookeeper.quorum", "hdp-101,hdp-102,hdp-103")
  con.set("hbase.zookeeper.property.clientPort", "2181") //zookeeper端口
  // 建立连接
  val conf: Connection = ConnectionFactory.createConnection(con)
  // 获取管理员
  val admin: Admin = conf.getAdmin
  // 创建表名称对象，通过这个对象才可以操作表
  val tableName: TableName = TableName.valueOf("student")
  // 通过连接对象获取表，操作表中数据
  val table: Table = conf.getTable(tableName)

  def main(args: Array[String]): Unit = {
     //create(List("info","data"))
     //insert("rk001","info","name","lisi001")
    /* inserAll(Array(
        ("rk001","info","name","lisi001"),("rk001","info","age","18"),
      ("rk002","data","name","lisi002"),("rk002","data","age","19")))*/

    // addColumnFamily("time")
     select()
    // selectRow("rk001")
   // deleteRow("rk001")
  }

  //todo:创建一个hbase表    一个属性，用于规定列族
  //create 'student','info'
  def create(columnFamily:List[String]): Unit ={
    //创建 hbase 表描述
    val tname = new HTableDescriptor(tableName)
    //添加列族，新建行模式当做列族
    columnFamily.foreach(x=>tname.addFamily(new HColumnDescriptor(x)))
    admin.createTable(tname)
    println("创建表成功")
  }

  //todo:添加一条数据
  //put 'student','rk001','info:name','zhangsan'
  def insert(row:String, columnFamily: String, column: String, value: String): Unit ={
    // 指定rowkey
    val puts = new Put(row.getBytes)
    puts.addColumn(columnFamily.getBytes, column.getBytes, value.getBytes)
    table.put(puts)
    println("添加数据成功")
  }

  //todo:添加多条数据
  def inserAll(array: Array[(String,String,String,String)]): Unit ={
    // java的 util
    val puts = new util.ArrayList[Put]()
    array.foreach(x=>{
      puts.add(new Put(x._1.getBytes()).addColumn(x._2.getBytes(),x._3.getBytes(),x._4.getBytes()))
    })
    table.put(puts)
    println("添加多条数据成功")
  }

  //todo:添加列族
  //alter 'student', NAME => 'data'
  def addColumnFamily(columnFamily: String): Unit = {
    // 表描述
    val tableDescriptor: HTableDescriptor = admin.getTableDescriptor(tableName)
    // 创建行描述
    val columnDescriptor = new HColumnDescriptor(columnFamily)
    // 设置版本数
    columnDescriptor.setVersions(2,5)
    // 将行描述添加到表描述中
    tableDescriptor.addFamily(columnDescriptor)
    // 修改表，需要名称对象，表描述对象
    admin.modifyTable(tableName,tableDescriptor)
    println("添加列族成功")
  }

  // todo: 查询表里数据
  def select(): Unit ={
    val scan = new Scan()
    val resultScanner: ResultScanner = table.getScanner(scan)
    var result: Result = resultScanner.next()
    while (result!=null){
      val cells: Array[Cell] = result.rawCells()
      cells.foreach(x=>println(
        Bytes.toString(CellUtil.cloneRow(x))+"  "+          // 获取row
          Bytes.toString(CellUtil.cloneFamily(x))+"  "+     // 获取列族
          Bytes.toString(CellUtil.cloneQualifier(x))+"  "+ // 获取列名
          Bytes.toString(CellUtil.cloneValue(x))))        // 获取值
      // 循环结束条件
      result = resultScanner.next()
    }


  }

  // todo: 根据行键查询数据
  def selectRow(row:String): Unit ={
    val get = new Get(row.getBytes)
    val result: Result = table.get(get)
    result.rawCells().foreach(println)
  }

  // todo: 根据行键删除数据
  def deleteRow(row:String): Unit ={
    val delete = new Delete(row.getBytes)
    table.delete(delete)
    println(s"${row}删除成功")
  }


  //关闭资源
  def close(): Unit = {
    if (admin != null)
      admin.close()
    if (conf != null)
      conf.close()
  }
}

