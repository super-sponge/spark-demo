#!/usr/bin/env bash

#FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

if [ -f "/etc/spark/conf/spark-env.sh" ]; then
    set -a
    . /etc/spark/conf/spark-env.sh
    set +a
fi

if [ -z "$SPARK_SCALA_VERSION" ]; then
  export SPARK_SCALA_VERSION="2.11"
fi

if [ -z "${HDP_VERSION}" ]; then

  if [  `command -v hdp-select` ]; then
    export HDP_VERSION=`hdp-select status | grep spark-client | awk -F " " '{print $3}'`
  else
    echo -e "command hdp-select is not found, please manually export HDP_VERSION in spark-env.sh or current environment" 1>&2
  fi
fi

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME=/usr/hdp/$HDP_VERSION/spark2
fi
