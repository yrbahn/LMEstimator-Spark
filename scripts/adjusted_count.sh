spark-submit --master spark://search-voice-spark1.dakao.io:6066 \
--deploy-mode cluster \
--class com.kakao.sparklm.SparkLM \
--executor-memory 17G \
--driver-memory 10G \
--conf spark.driver.maxResultSize=10g \
--conf spark.hadoop.validateOutputSpecs=false \
--conf spark.network.timeout=2000s \
--conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:+UseCompressedOops" \
http://dict6.dialoid.com/sparklm-assembly-0.1.jar --command adjusted_count --config http://dict6.dialoid.com/application.conf 
