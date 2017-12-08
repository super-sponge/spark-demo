package utils

import java.util.LinkedList
import java.sql.DriverManager
import java.sql.Connection
import java.util

import org.slf4j.LoggerFactory

/**
  * 不建议使用，作为演示
  */
object DbConnectionPool {
  val logger = LoggerFactory.getLogger(this.getClass)
  private val max_connection = "5" //连接池总数
  private val connection_num = "2" //初始连接池
  private var current_num = 0   //当前链接池已有连接数
  private val pools = new util.LinkedList[Connection]() //连接池
  private val driver = "com.mysql.jdbc.Driver"
  private val url = "jdbc:mysql://sdc1.sefon.com:3306/db"
  private val username = "db"
  private val password = "Dbdb123_"
  var connectionMqcrm: Connection = null
  /*
  * 加载驱动
  * */

  private def before(): Unit = {
    if (current_num > max_connection.toInt && pools.isEmpty){
      print("bussiness")
      Thread.sleep(30)
      before()
    }else{
      Class.forName(driver)
    }
  }
  /*
  * 链接认证
  * */
  private def initConn():Connection = {
    val conn = DriverManager.getConnection(url,username,password)
    conn
  }
  /*
  * 初始化连接池
  * */
  private def initConnectPool():LinkedList[Connection] = {
    AnyRef.synchronized({
      if (pools.isEmpty()){
        before()
        for (i <- 1 to connection_num.toInt){
          pools.push(initConn())
          current_num += 1
        }
      }
      pools
    })
  }

  /*
  * 获取链接
  * */

  def getConn():Connection = {
    initConnectPool()
    pools.poll()
  }

  /*
  * 释放链接
  * */

  def releaseCon(con:Connection): Unit ={
    pools.push(con)
  }

  def main(args : Array[String]): Unit = {

    val sql = "insert into wordcount(word, number) values (?,?)"
    val conn = DbConnectionPool.getConn()
    if (conn != null) {
      println("process data...")
      println("insert data ...")
      insert(conn, sql, ("hello", 4))
    }

    def insert(conn: Connection, sql: String, data: (String, Int)): Unit = {
      try {
        val ps = conn.prepareStatement(sql)
        ps.setString(1, data._1)
        ps.setInt(2, data._2)
        ps.executeUpdate()
        ps.close()
      } catch {
        case e: Exception =>
          logger.error("Error in execution of insert. " + e.getMessage)
      }
    }
  }

}