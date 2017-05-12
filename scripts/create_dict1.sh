spark-submit --master spark://search-voice-spark1.dakao.io:7077 \
--deploy-mode client \
--class com.kakao.sparklm.SparkLM \
--executor-memory 20G \
--executor-cores 3 \
/Users/roy/work/sparklm/target/scala-2.11/sparklm-assembly-0.1.jar --command create_dict --config http://dict6.dialoid.com/application.conf  
