package streaming


import java.sql.Connection

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.slf4j.LoggerFactory
import utils.{ConnectionPool, DbConnectionPool}

object SqlKafkaWordCount {
  val logger = LoggerFactory.getLogger(this.getClass)
  val batchDuration: Int = 10
  val sql = "insert into wordcount(word, number) values (?,?)"

  def KafkaWordCountStreamContext(zkQuorum: String,
                                  groupId: String,
                                  topics: String,
                                  numThreads: Int,
                                  securityProtocol: String ): StreamingContext = {

    val sparkConf = new SparkConf().setAppName("SqlKafkaWordCount");
    val ssc = new StreamingContext(sparkConf, Seconds(batchDuration))
    ssc.checkpoint("sqlkafkaworkcount/checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaParams = Map[String, String](
      "security.protocol" -> securityProtocol,
      "zookeeper.connect" -> zkQuorum, "group.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "10000")

    val lines = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topicMap,
      StorageLevel.MEMORY_AND_DISK_SER_2).repartition(10)

    lines.checkpoint(Seconds(5*batchDuration))
    val words = lines
    words.foreachRDD((rdd: RDD[(String, String)], time: Time) => {
      // Get the singleton instance of SQLContext
      val rdd1 = rdd.filter(_._1 == "tb1").map(_._2)
      val rdd2 = rdd.filter(_._1 == "tb2").map(_._2)
//      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      val sqlContext = HiveSQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      // Convert RDD[String] to RDD[case class] to DataFrame
      val wordsDataFrame1 = rdd1.map {
        w =>
          w.split(",") match {
            case Array(x,y) => Record(x.toInt, y)
            case _ => Record(1, w)
          }
      }.toDF()

      val wordsDataFrame2 = rdd2.map {
        w =>
          w.split(",") match {
            case Array(x,y) => Record(x.toInt, y)
            case _ => Record(1, w)
          }
      }.toDF()
      // Register as table
      wordsDataFrame1.registerTempTable("tb1")
      wordsDataFrame2.registerTempTable("tb2")

      // Do word count on table using SQL and print it
      val wordCountsDataFrame =
        sqlContext.sql("select 'tb1' as tb, count(*) as total from tb1 union select 'tb2' as tb, count(*) as total from tb2 " +
          "union select 'tmp' as tb, count(*) as total  from tmp")
      println(s"========= $time =========")
      wordCountsDataFrame.show()

      // 错误代码
//      val conn = DbConnectionPool.getConn()
//      println("get connection ...")
//      wordCountsDataFrame.foreach{ record =>
//        println("insert data ")
//        insert(conn, sql, (record.getString(0), record.getInt(1)))
//      }
//      DbConnectionPool.releaseCon(conn)

      //正确代码1
//      wordCountsDataFrame.foreachPartition { rdd =>
//        val conn = DbConnectionPool.getConn()
//        rdd.foreach { record =>
//          println("insert data " + record)
//          insert(conn, sql, (record.getString(0), record.getLong(1).toInt))
//        }
//        DbConnectionPool.releaseCon(conn)
//      }

      //正确代码2 推荐使用
      wordCountsDataFrame.foreachPartition { rdd =>
        val conn = ConnectionPool.getConnection.orNull
        if (conn != null) {
          rdd.foreach { record =>
            println("insert data " + record)
            insert(conn, sql, (record.getString(0), record.getLong(1).toInt))
          }
          ConnectionPool.closeConnection(conn)
        }
      }

    })

    ssc
  }

  case class Record(id:Int, word: String)

  /** Lazily instantiated singleton instance of SQLContext */
  object SQLContextSingleton {

    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }

  object HiveSQLContextSingleton {

    @transient private var instance: HiveContext = _

    def getInstance(sparkContext: SparkContext): HiveContext  = {
      if (instance == null) {
        instance = new HiveContext(sparkContext)
      }
      instance
    }
  }


  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads> " +
        "<securityProtocol>")
      System.exit(1)
    }

    val Array(zkQuorum, groupId, topics, numThreads, securityProtocol) =
      if (args.length > 4) args else args :+ "PLAINTEXT"

    val ssc = KafkaWordCountStreamContext(zkQuorum, groupId, topics, numThreads.toInt, securityProtocol)

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
