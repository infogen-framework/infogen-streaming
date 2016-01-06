#!/bin/bash
#UAT
hadoop jar infogen_streaming_0.8.2.2-jar-with-dependencies.jar \
-app_name infogen_tracking_uat \
-output hdfs://spark101:8020/infogen/output_uat/ \
-zookeeper 172.16.8.97:2181,172.16.8.98:2181,172.16.8.99:2181 \
-topic infogen_topic_tracking \
-group infogen_etl \
-mapper com.infogen.etl.Kafka_To_Hdfs_Mapper   \
-container_memory 256 \
-am_memory 256 \
-num_containers 5 

#无锡A
hadoop jar infogen_streaming_0.8.2.2-jar-with-dependencies.jar \
-app_name infogen_tracking_wuxia \
-output hdfs://spark101:8020/infogen/output_wuxia/ \
-zookeeper 172.16.1.50:2181,172.16.1.51:2181,172.16.1.52:2181 \
-topic infogen_topic_tracking \
-group infogen_etl \
-mapper com.infogen.etl.Kafka_To_Hdfs_Mapper   \
-container_memory 256 \
-am_memory 256 \
-num_containers 5 

#无锡B
hadoop jar infogen_streaming_0.8.2.2-jar-with-dependencies.jar \
-app_name infogen_tracking_wixib \
-output hdfs://spark101:8020/infogen/output_wuxib/ \
-zookeeper 172.16.1.150:2181,172.16.1.151:2181,172.16.1.152:2181 \
-topic infogen_topic_tracking \
-group infogen_etl \
-mapper com.infogen.etl.Kafka_To_Hdfs_Mapper   \
-container_memory 256 \
-am_memory 256 \
-num_containers 5 

