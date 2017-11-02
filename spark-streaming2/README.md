#spark stream kafka
    本程序在hdp 2.6.2 上测试通过
## 环境初始化    
### topic 创建
    cd /usr/hdp/current/kafka-broker
    kinit -kt /etc/security/keytabs/client.sefon.keytab  kafka/sefon@SEFON.COM
    ./bin/kafka-topics.sh --zookeeper sdc2.sefon.com:2181 --create --topic top1 --partitions 2 --replication-factor 2
    ./bin/kafka-topics.sh --zookeeper sdc2.sefon.com:2181 --create --topic top2 --partitions 2 --replication-factor 2
    
### 测试topic正常处理数据
    cd /usr/hdp/current/kafka-broker
    
    kinit -kt /etc/security/keytabs/client.sefon.keytab dev1/sefon@SEFON.COM
    ./bin/kafka-console-producer.sh --broker-list sdc1.sefon.com:6667,sdc2.sefon.com:6667 --topic top1 --security-protocol SASL_PLAINTEXT
    ./bin/kafka-console-consumer.sh  --bootstrap-server sdc1.sefon.com:6667,sdc2.sefon.com:6667  --topic top1 --security-protocol SASL_PLAINTEXT --new-consumer
    

## 程序运行
    运行实时处理流
    ./run-sparkstream-jad.sh  streaming.KafkaWordCount sdc2.sefon.com:2181 group1 top1,top2 1 SASL_PLAINTEXT
    运行产生消息
    ./send_message.sh sdc1.sefon.com:6667,sdc2.sefon.com:6667 top1 10 10 SASL_PLAINTEXT
    
