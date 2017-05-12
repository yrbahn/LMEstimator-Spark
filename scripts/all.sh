spark-submit --master spark://search-voice-spark1.dakao.io:6066 \
--deploy-mode cluster \
--class com.kakao.sparklm.SparkLM \
--executor-memory 5G \
--driver-memory 5G \
--conf spark.hadoop.validateOutputSpecs=false \
--conf spark.network.timeout=4000s \
http://dict6.dialoid.com/sparklm-assembly-0.1.jar --command all --config http://dict6.dialoid.com/application.conf  
