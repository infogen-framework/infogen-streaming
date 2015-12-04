package com.infogen.etl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;

import com.infogen.yarn3.RMCallbackHandler;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

/**
 * 基于kafka的日志消息接收处理器
 * 
 * @author larry/larrylv@outlook.com/创建时间 2015年8月3日 上午11:16:14
 * @since 1.0
 * @version 1.0
 */
public class InfoGen_ETL_Tracking_Kafka {
	private static Logger LOGGER = Logger.getLogger(InfoGen_ETL_Tracking_Kafka.class);
	/**
	 * infogen 定时对加载服务和监听纠错 调用程序不要使用
	 */
	public static ExecutorService executors = Executors.newCachedThreadPool((r) -> {
		Thread thread = Executors.defaultThreadFactory().newThread(r);
		thread.setDaemon(true);
		return thread;
	});

	public static void consume(String zookeepers, String group, String topic, Integer concurrent, InfoGen_ETL_Tracking_Kafka_Handle handle) {
		Properties props = new Properties();
		// zookeeper 配置
		props.put("zookeeper.connect", zookeepers);
		// group 代表一个消费组
		props.put("group.id", group);
		// zk连接超时
		props.put("zookeeper.session.timeout.ms", "10000");
		props.put("zookeeper.sync.time.ms", "2000"); // 从200修改成2000 太短有rebalance错误
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");// 2个合法的值"largest"/"smallest",默认为"largest",此配置参数表示当此groupId下的消费者,在ZK中没有offset值时(比如新的groupId,或者是zk数据被清空),consumer应该从哪个offset开始消费.largest表示接受接收最大的offset(即最新消息),smallest表示最小offset,即从topic的开始位置消费所有消息.
		// 序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);

		StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
		StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, concurrent);

		Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
		for (KafkaStream<String, String> kafkaStream : consumerMap.get(topic)) {
			executors.submit(() -> {
				ConsumerIterator<String, String> it = kafkaStream.iterator();
				while (it.hasNext()) {
					handle.handle_event(it.next().message());
				}
			});
		}
	}

	public static void main(String[] args) {
		LOGGER.info("start-----------------------------------------------");
		InfoGen_ETL_Tracking_Kafka.consume("172.16.8.97,172.16.8.98,172.16.8.99", "infogen_etl_tracking", "infogen_topic_tracking", 3, (message) -> {
			LOGGER.info(message);
		});
		try {
			Thread.currentThread().join();
		} catch (InterruptedException e) {
			e.printStackTrace();
			LOGGER.error("", e);
		}
		LOGGER.info("end------------------------------------------------------");
	}

}
