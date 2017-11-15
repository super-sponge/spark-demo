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
    ./bin/kafka-console-producer.sh --broker-list sdc1.sefon.com:6667,sdc2.sefon.com:6667 --topic top1 --security-protocol PLAINTEXTSASL 
    ./bin/kafka-console-consumer.sh  --bootstrap-server sdc1.sefon.com:6667,sdc2.sefon.com:6667  --topic top1 --security-protocol PLAINTEXTSASL --new-consumer
    
## 程序部署
    tar -zxf spark-streaming1-1.0-SNAPSHOT-package.tar.gz
    tar -zxf spark-streaming2-1.0-SNAPSHOT-package.tar.gz
    对于streaming1 配置文件conf下有三个
    client.sefon.keytab    访问kafka指定的keytab
    kafka_jaas.conf        访问kafka的jaas文件
    spark.sefon.keytab     提交spark时使用的keytab(提交时不能使用client.sefon.keytab,会报错)
    对于streaming2 配置文件在streaming１的基础上添加了client_jaas.conf用于产生消息,此配置文件里面keytab一定要配置绝对路径
    
## 程序运行
    运行实时处理流(streaming1/2　都是一样的命令)
    ./run-sparkstream-jad.sh  streaming.KafkaWordCount sdc2.sefon.com:2181 group1 top1,top2 1 PLAINTEXTSASL 
    运行产生消息（协议如果没有启用kerberos 使用PLAINTEXT）
    ./send_message.sh sdc1.sefon.com:6667,sdc2.sefon.com:6667 top1 10 10 PLAINTEXTSASL
## 备注
    1. 程序运行状态需要查看具体container的本地日志,才能确定是否成功
  
    


    
