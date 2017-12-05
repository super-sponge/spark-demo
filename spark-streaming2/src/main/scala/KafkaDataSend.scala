import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

/**
  * 模拟向topic 发送数据，数据格式key 为表名，数据为id,value用","分割
  */
object KafkaDataSend {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<tableName> <numberMessage> <securityProtocol>")
      System.exit(1)
    }

    val Array(brokers, topic, tableName, numberMessage, securityProtocol) =
      if (args.length > 4) args else args :+ "PLAINTEXT"

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put("security.protocol", securityProtocol)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Send some messages
    val numbersEvent = numberMessage.toInt
      (1 to numbersEvent).foreach { messageNum =>
        val value = Random.nextInt(10).toString + ",word" + Random.nextInt(numbersEvent * 10).toString;
        val message = new ProducerRecord[String, String](topic, null, tableName, value)
        producer.send(message)
      Thread.sleep(500)
    }
  }
}
