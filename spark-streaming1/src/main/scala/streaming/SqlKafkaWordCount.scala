package streaming


import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object SqlKafkaWordCount {
  val batchDuration: Int = 10

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
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      // Convert RDD[String] to RDD[case class] to DataFrame
      val wordsDataFrame1 = rdd1.map {
        w =>
          val splits = w.split(",")
          if (splits.length < 2) {
            Record(1, w)
          } else {
            Record(splits(0).toInt, splits(1))
          }
      }.toDF()

      val wordsDataFrame2 = rdd2.map {
        w =>
          val splits = w.split(",")
          if (splits.length < 2) {
            Record(1, w)
          } else {
            Record(splits(0).toInt, splits(1))
          }
      }.toDF()
      // Register as table
      wordsDataFrame1.registerTempTable("tb1")
      wordsDataFrame2.registerTempTable("tb2")

      // Do word count on table using SQL and print it
      val wordCountsDataFrame =
        sqlContext.sql("select 'tb1' as tb, count(*) as total from tb1 union select 'tb2' as tb, count(*) as total from tb2")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
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
}
