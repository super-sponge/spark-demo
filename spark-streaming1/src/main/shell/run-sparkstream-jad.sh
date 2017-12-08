#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

. "$FWDIR"/bin/load-spark-env.sh


if [ -n "$1" ]; then
  RUN_CLASS="$1"
  shift
else
  echo "Usage: ./bin/run-sparkstream.sh  <example-class> [example-args]" 1>&2
  echo "  - set MASTER=XX to use a specific master" 1>&2
  exit 1
fi

export SPARK_RUN_JAR="$FWDIR"/spark-streaming1-1.0-SNAPSHOT.jar
#RUN_MASTER=${MASTER:-"local[*]"}
RUN_MASTER=${MASTER:-"yarn-client"}
#RUN_MASTER=${MASTER:-"yarn-cluster"}


for file in $FWDIR/lib/*
do
JARS=$file,$JARS
done

exec "$SPARK_HOME"/bin/spark-submit \
  --master $RUN_MASTER \
  --num-executors 4 \
  --jars $JARS \
  --keytab $FWDIR/conf/spark.sefon.keytab \
  --principal dev1/sefon@SEFON.COM \
  --files  /usr/hdp/current/spark-client/conf/hive-site.xml,$FWDIR/conf/kafka_jaas.conf,$FWDIR/conf/client.sefon.keytab \
  --driver-java-options "-Djava.security.auth.login.config=kafka_jaas.conf" \
  --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=kafka_jaas.conf" \
  --class $RUN_CLASS \
  "$SPARK_RUN_JAR" \
  "$@"

