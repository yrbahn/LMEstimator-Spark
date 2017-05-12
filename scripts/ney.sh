#!/bin/bash

if [ $# -ne 1 ]; then
  echo  "a"
  exit 0
fi

CMD = "adjusted_count"

if [ $1 -eq "adjusted_count"]; then
  CMD = $1
else
  exit 1
fi

spark-submit --master spark://search-voice-spark1.dakao.io:6066 \
--deploy-mode cluster \
--class com.kakao.sparklm.SparkLM \
--executor-memory 20G \
--driver-memory 20G \
--executor-cores 3 \
--conf spark.driver.maxResultSize=20g \
--conf spark.hadoop.validateOutputSpecs=false \
--conf spark.network.timeout=2000s \
--conf spark.executor.extraJavaOptions='-XX:+UseG1GC -XX:+UseCompressedOops' \
--conf spark.io.compression.codec=snappy \
--conf spark.driver.extraClassPath=/data1/daum/alluxio-1.3.0/core/client/target/alluxio-1.3.0-spark-client-jar-with-dependencies.jar \
--conf spark.executor.extraClassPath=/data1/daum/alluxio-1.3.0/core/client/target/alluxio-1.3.0-spark-client-jar-with-dependencies.jar \
http://dict6.dialoid.com/sparklm-assembly-0.1.jar --command $CMD --config http://dict6.dialoid.com/application.conf 
