package com.infogen.kafka08;

import java.util.Properties;

import org.apache.log4j.Logger;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * 启动kafka生产者
 * 
 * @author larry/larrylv@outlook.com/创建时间 2015年8月3日 上午11:17:34
 * @since 1.0
 * @version 1.0
 */
public class InfoGen_Producer {
	private static Logger LOGGER = Logger.getLogger(InfoGen_Producer.class);

	private static class InnerInstance {
		public static final InfoGen_Producer instance = new InfoGen_Producer();
	}

	public static InfoGen_Producer getInstance() {
		return InnerInstance.instance;
	}

	private InfoGen_Producer() {
	}

	public static void main(String[] args) {
		InfoGen_Producer instance = InfoGen_Producer.getInstance();
		instance.start("172.16.8.97:10086,172.16.8.98:10086,172.16.8.99:10086");
		for (Integer i = 0; i < 100; i++) {
			instance.send("test_simple_example", i.toString(), i.toString());
		}
	}

	private Producer<String, String> producer;

	// 启动kafka生产者
	public InfoGen_Producer start(String brokers) {
		if (brokers == null || brokers.trim().isEmpty()) {
			LOGGER.error("没有配置 kafka");
			return this;
		}
		if (producer == null) {
			// 设置配置属性
			Properties props = new Properties();
			props.put("metadata.broker.list", brokers);
			props.put("serializer.class", "kafka.serializer.StringEncoder");
			// 触发acknowledgement机制，否则是fire and forget，可能会引起数据丢失
			// 值为0,1,-1,可以参考
			// http://kafka.apache.org/08/configuration.html
			props.put("request.required.acks", "1");
			// key.serializer.class默认为serializer.class
			// 如果topic不存在，则会自动创建，默认replication-factor为1，partitions为0
			ProducerConfig config = new ProducerConfig(props);
			// 创建producer
			producer = new Producer<String, String>(config);
		}
		return this;
	}

	// 关闭一个kafka生产者
	public void close() {
		if (producer != null) {
			producer.close();
		}
	}

	// 发送消息 如果生产者没有初始化只写一次日志
	public void send(String topic, String key, String message) {
		if (producer != null) {
			try {
				producer.send(new KeyedMessage<String, String>(topic, key, message));
			} catch (Exception e) {
				LOGGER.warn("kafka 发送失败", e);
			}
		} else {
			LOGGER.warn("kafka未初始化");
		}
	}
}