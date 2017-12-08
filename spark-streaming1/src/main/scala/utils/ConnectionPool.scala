package utils

import java.sql.Connection

import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import org.slf4j.LoggerFactory

/**
  * 建议使用,该缓存池采用BoneCP作为缓冲池
  */
object ConnectionPool {
  val logger = LoggerFactory.getLogger(this.getClass)

  //连接池配置
  private val connectionPool: Option[BoneCP] = {
    try{
      Class.forName("com.mysql.jdbc.Driver")
      val config = new BoneCPConfig()
      config.setJdbcUrl("jdbc:mysql://sdc1.sefon.com:3306/db")
      config.setUsername("db")
      config.setPassword("Dbdb123_")
      config.setLazyInit(true)

      config.setMinConnectionsPerPartition(3)
      config.setMaxConnectionsPerPartition(5)
      config.setPartitionCount(5)
      config.setCloseConnectionWatch(true)
      config.setLogStatementsEnabled(false)
      Some(new BoneCP(config))
    }catch {
      case exception: Exception =>
        logger.warn("Create Connection Error: \n" + exception.printStackTrace())
        println(exception.printStackTrace())
        None
    }
  }

  // 获取数据库连接
  def getConnection: Option[Connection] = {
    connectionPool match {
      case Some(pool) => Some(pool.getConnection)
      case None => None
    }
  }

  // 释放数据库连接
  def closeConnection(connection:Connection): Unit = {
    if (!connection.isClosed) {
      connection.close()
    }
  }

  def main(args : Array[String]): Unit = {

    val sql = "insert into wordcount(word, number) values (?,?)"
    val conn = ConnectionPool.getConnection.orNull
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
