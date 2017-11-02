#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

JARS=$FWDIR/spark-streaming2-1.0-SNAPSHOT.jar

for file in $FWDIR/libext/*
do
JARS=$file:$JARS
done

java_cmd=/usr/jdk64/jdk1.8.0_112/bin/java

cmd="$java_cmd -cp $JARS -Djava.security.auth.login.config=$FWDIR/conf/client_jaas.conf KafkaWordCountProducer $@"
echo "$cmd"
exec $cmd
