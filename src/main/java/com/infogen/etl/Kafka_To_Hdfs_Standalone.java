package com.infogen.etl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.infogen.kafka.InfoGen_Container;
import com.infogen.mapper.InfoGen_Mapper;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月21日 下午1:12:07
 * @since 1.0
 * @version 1.0
 */
public class Kafka_To_Hdfs_Standalone {
	private static final Log LOGGER = LogFactory.getLog(InfoGen_Container.class);
	///////////////////////////////////////////////////////////////////////////////

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws InterruptedException {
		String zookeeper = "172.16.8.97:2181,172.16.8.98:2181,172.16.8.99:2181";
		String topic = "infogen_topic_tracking";
		String group = "infogen_etl";
		String mapper_clazz = "com.infogen.etl.Kafka_To_Hdfs_Mapper";
		String parameters = "hdfs://spark101:8020/infogen/output/";
		if (topic == null || zookeeper == null || group == null || mapper_clazz == null) {
			LOGGER.error("参数不能为空");
			return;
		}

		for (int i = 0; i < 5; i++) {
			new Thread(() -> {
				try {
					InfoGen_Container infogen_container = new InfoGen_Container();
					infogen_container.run(zookeeper, topic, group, (Class<? extends InfoGen_Mapper>) Class.forName(mapper_clazz), parameters);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}).start();
		}
		Thread.currentThread().join();
	}

}
