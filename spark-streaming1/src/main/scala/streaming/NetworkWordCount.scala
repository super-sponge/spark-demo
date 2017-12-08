package streaming

import java.sql.Connection

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import utils.ConnectionPool

object NetworkWordCount {
  val logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }


    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // Create a socket stream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    // Note that no duplication in storage level only for running locally.
    // Replication necessary in distributed scenario for fault tolerance.
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    val sql = "insert into wordcount(word, number) values (?,?)"
    wordCounts.foreachRDD { rdd =>
      println("this is driver program")
      if(! rdd.isEmpty()) {
        rdd.foreachPartition { pr => {
          println("get connect..")
          val conn = ConnectionPool.getConnection.orNull
          if (conn != null) {
            println("process data...")
            pr.foreach(data => {
              println("insert data ...")
              insert(conn, sql, data)
            })
            ConnectionPool.closeConnection(conn)
          } else {
            println("conn is null")
          }
        }
        }
      }

    }
//    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
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
