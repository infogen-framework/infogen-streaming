package com.infogen.kafka;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import com.infogen.exception.AutoOffsetReset_Exception;
import com.infogen.exception.NoPartition_Exception;
import com.infogen.hdfs.InfoGen_KafkaLZOOutputFormat;
import com.infogen.hdfs.InfoGen_OutputFormat;
import com.infogen.mapper.InfoGen_Mapper;
import com.infogen.zookeeper.InfoGen_ZooKeeper;

/**
 * 使用原生API的kafka消费者 <br>
 * 调用自定义mapper方法过滤消息 <br>
 * 使用commit_offset确保消息被写入到hdfs（文件以.finish后缀）
 * 
 * @author larry/larrylv@outlook.com/创建时间 2015年11月30日 下午2:28:09
 * @since 1.0
 * @version 1.0
 */
public class InfoGen_Consumer {
	private static Logger LOGGER = Logger.getLogger(InfoGen_Consumer.class);

	public enum AUTO_OFFSET_RESET {
		earliest, latest, none
	}

	private InfoGen_ZooKeeper infogen_zookeeper = new InfoGen_ZooKeeper();

	private String brokers;
	private String topic;
	private String group;
	private Integer partition;
	private Long offset;
	/* 当没有指定起始offset，且在zookeeper中也没有保存时，从何处开始获取消息 largest：最新的，smallest：最早的 */
	private String auto_offset_reset = AUTO_OFFSET_RESET.earliest.name();
	/* 完成事务-写入到hdfs且流关闭后更新commit_offset,当程序出现异常重新加载时会从此处开始获取消息，并覆盖之前写到hdfs上的数据 */
	private Long commit_offset;

	public InfoGen_Consumer() {
	}

	public Long start(final String zookeeper, final String topic, final String group, final Long start_offset, final String auto_offset_reset, final Class<? extends InfoGen_Mapper> infogen_mapper_class, final String output, final String parameters) throws IOException, InstantiationException, NoPartition_Exception, IllegalAccessException, AutoOffsetReset_Exception {
		this.infogen_zookeeper.start_zookeeper(zookeeper, null);

		this.topic = topic;
		this.group = group;
		this.offset = start_offset;
		this.commit_offset = start_offset;
		this.auto_offset_reset = auto_offset_reset;

		try {
			// partition
			Integer partition = -1;
			Integer partitions_size = infogen_zookeeper.get_childrens("/brokers/topics/" + topic + "/partitions").size();
			for (int i = 0; i < partitions_size; i++) {
				infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.offset(topic, group, i), CreateMode.PERSISTENT);
				String create = infogen_zookeeper.create(InfoGen_ZooKeeper.partition(topic, group, i), null, CreateMode.EPHEMERAL);
				if (create == null || create.equals(Code.NODEEXISTS.name())) {
					LOGGER.info("#创建partition失败:" + i);
				} else {
					LOGGER.info("#获取partition成功:" + i);
					partition = i;
					break;
				}
			}
			if (partition == -1) {
				LOGGER.info("#创建partition失败  退出ETL");
				throw new NoPartition_Exception();
			} else {
				this.partition = partition;
			}
			LOGGER.info("#partition为：" + partition);

			// offset
			if (start_offset == null || start_offset < 0) {
				String get_offset = infogen_zookeeper.get_data(InfoGen_ZooKeeper.offset(topic, group, partition));
				if (get_offset != null) {
					Long save_offset = Long.valueOf(get_offset);
					this.offset = save_offset;
					this.commit_offset = save_offset;
					LOGGER.info("#使用zookeeper 中 offset为:" + commit_offset);
				}
			}
			if (offset == null) {
				if (auto_offset_reset.equals(AUTO_OFFSET_RESET.earliest.name())) {
					offset = -1l;
				} else if (auto_offset_reset.equals(AUTO_OFFSET_RESET.latest.name())) {
					offset = Long.MAX_VALUE;
				} else if (auto_offset_reset.equals(AUTO_OFFSET_RESET.none.name())) {
					throw new AutoOffsetReset_Exception();
				} else {
					throw new AutoOffsetReset_Exception();
				}
			}
			LOGGER.info("#commit_offset为：" + commit_offset);

			// brokers
			StringBuilder brokers_sb = new StringBuilder();
			for (String id : infogen_zookeeper.get_childrens("/brokers/ids")) {
				for (String kv : infogen_zookeeper.get_data("/brokers/ids/" + id).replace("{", "").replace("}", "").replace("\"", "").split(",")) {
					String key = kv.split(":")[0];
					String value = kv.split(":")[1];
					if (key.equals("host")) {
						brokers_sb.append(value).append(":");
					} else if (key.equals("port")) {
						brokers_sb.append(value).append(",");
					} else {
					}
				}
			}
			this.brokers = brokers_sb.substring(0, brokers_sb.length() - 1);
			LOGGER.info("#broker为：" + brokers);

			// InfoGen_Mapper
			InfoGen_Mapper mapper = infogen_mapper_class.newInstance();
			mapper.config(topic, partition, output, parameters);

			// InfoGen_OutputFormat
			InfoGen_OutputFormat infogen_kafkalzooutputformat = new InfoGen_KafkaLZOOutputFormat(partition);

			LOGGER.info("#执行ETL offset为：" + start_offset);
			run(mapper, infogen_kafkalzooutputformat);
			LOGGER.error("#ETL中断 : commit_offset-" + commit_offset + " offset-" + offset);
			return commit_offset;
		} finally {
			infogen_zookeeper.stop_zookeeper();
		}
	}

	private void run(final InfoGen_Mapper mapper, final InfoGen_OutputFormat infogen_kafkalzooutputformat) throws IOException {
		LOGGER.info("#获取指定Topic partition的元数据：topic-" + topic + " partition-" + partition);
		Properties props = new Properties();
		props.put("bootstrap.servers", brokers);
		props.put("group.id", group);
		props.put("enable.auto.commit", "false");
		props.put("auto.offset.reset", auto_offset_reset);
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		@SuppressWarnings("resource")
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		TopicPartition topic_partition = new TopicPartition(topic, partition);
		consumer.assign(Arrays.asList(topic_partition));

		consumer.seekToBeginning(Arrays.asList(topic_partition));
		Long earliestOffset = consumer.position(topic_partition);
		consumer.seekToEnd(Arrays.asList(topic_partition));
		Long latestOffset = consumer.position(topic_partition);

		if (offset < earliestOffset) {
			LOGGER.warn("#offset不存在-从最早的offset开始获取");
			offset = earliestOffset;
		} else if (offset > latestOffset) {
			LOGGER.warn("#offset不存在-从最后的offset开始获取");
			offset = latestOffset;
		}
		infogen_kafkalzooutputformat.setStart_offset(offset);
		LOGGER.info("#offset：" + offset + ":earliestOffset-" + earliestOffset + " latestOffset-" + latestOffset);

		Long fetch_size = 0l;
		Long max_uncommit_size = 1024 * 1024 * 64l;

		Integer max_number_io = 20;

		long millis = Clock.systemUTC().millis();
		Long max_uncommit_millis = 1000 * 60 * 30l;

		for (;;) {
			consumer.seek(topic_partition, offset);
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				String value = record.value();
				offset = record.offset() + 1;// nextOffset
				fetch_size += value.length();
				try {
					mapper.mapper(offset, record.key(), value, infogen_kafkalzooutputformat);
				} catch (IOException e) {
					LOGGER.error("#创建hdfs流失败:", e);
					throw e;
				} catch (IllegalArgumentException e) {
					LOGGER.error("#获取hdfs Path失败:", e);
					throw e;
				}
			}

			// 更新offset
			long current_millis = Clock.systemUTC().millis();
			if (fetch_size >= max_uncommit_size || (current_millis - millis) >= max_uncommit_millis || infogen_kafkalzooutputformat.number_io() >= max_number_io) {
				if (!infogen_kafkalzooutputformat.close_all()) {
					LOGGER.error("#关闭流失败，重试: commit_offset-" + commit_offset + " offset-" + offset);
					return;
				} else {
					LOGGER.info("#关闭流成功 : commit_offset-" + commit_offset + " offset-" + offset);
				}

				for (;;) {
					Stat set_data = infogen_zookeeper.set_data(InfoGen_ZooKeeper.offset(topic, group, partition), offset.toString().getBytes(), -1);
					if (set_data != null) {
						commit_offset = offset;
						infogen_kafkalzooutputformat.setStart_offset(offset);
						LOGGER.info("#更新offset成功 : commit_offset-" + commit_offset + " offset-" + offset);
						break;
					} else {
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							LOGGER.error("", e);
						}
					}
				}

				fetch_size = 0l;
				millis = current_millis;
			}
			// END 更新offset

			if (records.isEmpty()) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					LOGGER.error("", e);
				}
			}
		}
	}
}