spark-submit --master spark://search-voice-spark1.dakao.io:6066 \
--deploy-mode cluster \
--class com.kakao.sparklm.SparkLM \
--executor-memory 20G \
--driver-memory 20G \
--executor-cores 3 \
--conf spark.hadoop.validateOutputSpecs=false \
--conf spark.network.timeout=2000s \
http://dict6.dialoid.com/sparklm-assembly-0.1.jar --command uninterp_prob --config http://dict6.dialoid.com/application.conf  
