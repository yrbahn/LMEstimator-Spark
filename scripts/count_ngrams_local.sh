spark-submit --master local[4] \
--class com.kakao.sparklm.SparkLM \
--conf spark.network.timeout=10000000 \
/Users/roy/work/sparklm/target/scala-2.11/sparklm-assembly-0.1.jar --command create_dict --config /Users/roy/work/sparklm/conf/application_local.conf  
