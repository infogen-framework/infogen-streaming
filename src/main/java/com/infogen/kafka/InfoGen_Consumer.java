package com.infogen.kafka;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import com.infogen.exception.NoPartition_Exception;
import com.infogen.hdfs.InfoGen_KafkaLZOOutputFormat;
import com.infogen.hdfs.InfoGen_OutputFormat;
import com.infogen.mapper.InfoGen_Mapper;
import com.infogen.zookeeper.InfoGen_ZooKeeper;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

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
		largest, smallest
	}

	private InfoGen_ZooKeeper infogen_zookeeper = new InfoGen_ZooKeeper();

	private String brokers;
	private String topic;
	private String group;
	private Integer partition;
	private Long offset;
	/* 当没有指定起始offset，且在zookeeper中也没有保存时，从何处开始获取消息 largest：最新的，smallest：最早的 */
	private String auto_offset_reset = AUTO_OFFSET_RESET.largest.name();
	/* 完成事务-写入到hdfs且流关闭后更新commit_offset,当程序出现异常重新加载时会从此处开始获取消息，并覆盖之前写到hdfs上的数据 */
	private Long commit_offset;

	public InfoGen_Consumer() {
	}

	public Long start(final String zookeeper, final String topic, final String group, final Long start_offset, final String auto_offset_reset, final Class<? extends InfoGen_Mapper> infogen_mapper_class, final String parameters) throws IOException, InstantiationException, NoPartition_Exception, IllegalAccessException {
		this.infogen_zookeeper.start_zookeeper(zookeeper, null);

		this.topic = topic;
		this.group = group;
		this.offset = start_offset;
		this.commit_offset = start_offset;
		if (auto_offset_reset.equals(AUTO_OFFSET_RESET.smallest.name())) {
			this.auto_offset_reset = auto_offset_reset;
		}

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
			mapper.config(topic, partition, parameters);

			// InfoGen_OutputFormat
			InfoGen_OutputFormat infogen_kafkalzooutputformat = new InfoGen_KafkaLZOOutputFormat(partition);
			infogen_kafkalzooutputformat.setCommit_offset(commit_offset);

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
		PartitionMetadata partition_metadata = findPartitionMetadata();
		if (partition_metadata == null) {
			LOGGER.error("Can't find metadata for Topic and Partition. Exiting");
			return;
		}

		if (partition_metadata.leader() == null) {
			LOGGER.error("Can't find Leader for Topic and Partition. Exiting");
			return;
		}

		String leaderBroker = partition_metadata.leader().host();
		LOGGER.info("#leaderBroker：" + leaderBroker);
		Integer leaderPort = partition_metadata.leader().port();
		LOGGER.info("#leaderPort：" + leaderPort);
		String clientId = "Client_" + topic + "_" + partition + "_" + Clock.systemUTC().millis();
		LOGGER.info("#clientId：" + clientId);

		SimpleConsumer consumer = new SimpleConsumer(leaderBroker, leaderPort, 100000, 64 * 1024, clientId);
		Long earliestOffset = getLastOffset(consumer, kafka.api.OffsetRequest.EarliestTime(), clientId);
		Long latestOffset = getLastOffset(consumer, kafka.api.OffsetRequest.LatestTime(), clientId);
		if (earliestOffset == null) {
			LOGGER.error("Can't find earliestOffset. Exiting");
			return;
		} else if (latestOffset == null) {
			LOGGER.error("Can't find latestOffset. Exiting");
			return;
		}

		if (offset == null || offset < 0) {// 没有指定offset且zookeeper中也没有保存
			if (auto_offset_reset.equals(AUTO_OFFSET_RESET.smallest.name())) {
				LOGGER.warn("#offset不存在-从最早的offset开始获取");
				offset = earliestOffset;
			} else {
				LOGGER.warn("#offset不存在-从最后的offset开始获取");
				offset = latestOffset;
			}
		} else if (offset < earliestOffset) {
			LOGGER.warn("#offset不存在-从最早的offset开始获取");
			offset = earliestOffset;
		} else if (offset > latestOffset) {
			LOGGER.warn("#offset不存在-从最后的offset开始获取");
			offset = latestOffset;
		}
		LOGGER.info("#offset：" + offset + ":earliestOffset-" + earliestOffset + " latestOffset-" + latestOffset);

		Integer num_errors = 0;
		Integer max_errors = 5;

		Long fetch_size = 0l;
		Integer fetch_block_size = 1024 * 1024 * 2;
		Long max_uncommit_size = 1024 * 1024 * 64l;

		Integer max_number_io = 20;

		long millis = Clock.systemUTC().millis();
		Long max_uncommit_millis = 1000 * 60 * 5l;
		// 最多10分钟或64M执行一次commit
		LOGGER.info("#开始ETL：单次获取字节数-" + fetch_block_size + " 最高重试fetch次数-" + max_errors + "  最多未commit消息大小-" + max_uncommit_size + "  最多未commit时间-" + max_uncommit_millis);

		try {
			for (;;) {
				// 单条消息超过fetch_size_ones 会返回一个空的fetchResponse
				FetchRequest req = new FetchRequestBuilder().clientId(clientId).addFetch(topic, partition, offset, fetch_block_size).build();
				FetchResponse fetchResponse = consumer.fetch(req);

				if (fetchResponse.hasError()) {
					num_errors++;

					Short code = fetchResponse.errorCode(topic, partition);
					LOGGER.error("#获取数据失败 from the Broker:" + leaderBroker + " Reason: " + code);
					if (code == ErrorMapping.OffsetOutOfRangeCode()) {
						LOGGER.error("#OffsetOutOfRangeCode:" + offset);
					}
					if (num_errors > max_errors) {
						return;
					}

					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						LOGGER.error("", e);
					}
				} else {
					num_errors = 0;

					// 遍历MESSAGE
					for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
						long currentOffset = messageAndOffset.offset();
						if (currentOffset < offset) {
							LOGGER.info("#Found an old offset: " + currentOffset + " Expecting: " + offset);
						} else {
							ByteBuffer payload = messageAndOffset.message().payload();
							int limit = payload.limit();
							byte[] bytes = new byte[limit];
							payload.get(bytes);

							try {
								mapper.mapper(offset, new String(bytes, "UTF-8"), infogen_kafkalzooutputformat);
							} catch (UnsupportedEncodingException e) {
								LOGGER.error("#kafka数据转换String失败:", e);
								throw e;
							} catch (IOException e) {
								LOGGER.error("#创建hdfs流失败:", e);
								throw e;
							} catch (IllegalArgumentException e) {
								LOGGER.error("#获取hdfs Path失败:", e);
								throw e;
							}

							fetch_size += limit;
							offset = messageAndOffset.nextOffset();
						}
					}
					// END 遍历MESSAGE
				}

				// 更新offset
				if (fetch_size >= max_uncommit_size || (Clock.systemUTC().millis() - millis) >= max_uncommit_millis || infogen_kafkalzooutputformat.number_io() >= max_number_io) {
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
							infogen_kafkalzooutputformat.setCommit_offset(commit_offset);
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
					millis = Clock.systemUTC().millis();
				}
				// END 更新offset
			}
		} finally {
			if (consumer != null) {
				consumer.close();
			}
		}
	}

	// 找到指定分区的元数据
	private PartitionMetadata findPartitionMetadata() {
		PartitionMetadata return_partition_metadata = null;
		for (String broker : brokers.split(",")) {
			String host = broker.split(":")[0];
			Integer port = Integer.valueOf(broker.split(":")[1]);

			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(host, port, 100000, 64 * 1024, "partitionLookup");
				TopicMetadataRequest request = new TopicMetadataRequest(Collections.singletonList(topic));
				TopicMetadataResponse response = consumer.send(request);

				for (TopicMetadata topicMetadata : response.topicsMetadata()) {
					for (PartitionMetadata partition_metadata : topicMetadata.partitionsMetadata()) {
						if (partition_metadata.partitionId() == partition) {
							return_partition_metadata = partition_metadata;
						}
					}
				}
			} catch (Exception e) {
				LOGGER.error("findPartitionMetadata :" + host + "-" + port + "-" + topic, e);
			} finally {
				if (consumer != null) {
					consumer.close();
				}
			}
		}
		return return_partition_metadata;
	}

	public Long getLastOffset(SimpleConsumer consumer, long whichTime, String clientId) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientId);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		if (response.hasError()) {
			LOGGER.error("#获取last offset失败. Reason: " + response.errorCode(topic, partition));
			return null;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}
}