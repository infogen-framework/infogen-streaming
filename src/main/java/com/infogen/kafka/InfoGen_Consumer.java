package com.infogen.kafka;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import com.infogen.hdfs.InfoGen_KafkaLZOOutputFormat;
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
 * @author larry/larrylv@outlook.com/创建时间 2015年11月30日 下午2:28:09
 * @since 1.0
 * @version 1.0
 */
public class InfoGen_Consumer {
	private static Logger LOGGER = Logger.getLogger(InfoGen_Consumer.class);

	enum AUTO_OFFSET_RESET {
		largest, smallest
	}

	private InfoGen_ZooKeeper infogen_zookeeper = new InfoGen_ZooKeeper();
	private InfoGen_Mapper mapper;
	private String brokers;
	private String topic;
	private Integer partition;
	private String auto_offset_reset = AUTO_OFFSET_RESET.largest.name();
	private Long offset;
	private Long commit_offset;// 完成事务-写入到hdfs且流关闭

	public InfoGen_Consumer() {
	}

	public Long start(String zookeeper, String brokers, String topic, Integer partition, Long offset, InfoGen_Mapper mapper, String auto_offset_reset) throws IOException {
		this.infogen_zookeeper.start_zookeeper(zookeeper, null);
		this.brokers = brokers;
		this.topic = topic;
		this.partition = partition;
		this.offset = offset;
		this.commit_offset = offset;
		this.mapper = mapper;
		if (auto_offset_reset.equals(AUTO_OFFSET_RESET.smallest.name())) {
			this.auto_offset_reset = auto_offset_reset;
		}

		try {
			run();
			return commit_offset;
		} catch (Exception e) {
			return commit_offset;
		} finally {
			infogen_zookeeper.stop_zookeeper();
		}
	}

	private void run() {
		// 获取指定Topic partition的元数据
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
		Integer port = partition_metadata.leader().port();
		String clientId = "Client_" + topic + "_" + partition + "_" + Clock.systemUTC().millis();

		SimpleConsumer consumer = new SimpleConsumer(leaderBroker, port, 100000, 64 * 1024, clientId);
		Long earliestOffset = getLastOffset(consumer, kafka.api.OffsetRequest.EarliestTime(), clientId);
		Long latestOffset = getLastOffset(consumer, kafka.api.OffsetRequest.LatestTime(), clientId);
		if (earliestOffset == null) {
			LOGGER.error("Can't find earliestOffset. Exiting");
			return;
		}
		if (latestOffset == null) {
			LOGGER.error("Can't find latestOffset. Exiting");
			return;
		}

		if (offset == null || offset == -1) {// 没有指定offset且zookeeper中也没有保存
			if (auto_offset_reset.equals(AUTO_OFFSET_RESET.smallest.name())) {
				LOGGER.warn("#offset不存在-从最早的offset开始获取:" + earliestOffset);
				offset = earliestOffset;
			} else {
				LOGGER.warn("#offset不存在-从最后的offset开始获取:" + latestOffset);
				offset = latestOffset;
			}
		} else if (offset < earliestOffset) {
			LOGGER.warn("#offset不存在-从最早的offset开始获取:" + earliestOffset);
			offset = earliestOffset;
		} else if (offset > latestOffset) {
			LOGGER.warn("#offset不存在-从最后的offset开始获取:" + latestOffset);
			offset = latestOffset;
		} else {

		}
		// offset = latestOffset;
		Integer num_errors = 0;
		Integer max_errors = 5;

		InfoGen_KafkaLZOOutputFormat infogen_kafkalzooutputformat = new InfoGen_KafkaLZOOutputFormat();
		infogen_kafkalzooutputformat.commit_offset = commit_offset;
		Long fetch_size = 0l;
		long millis = Clock.systemUTC().millis();
		try {

			for (;;) {
				// 读取大小为64M 超过会返回一个空的fetchResponse
				FetchRequest req = new FetchRequestBuilder().clientId(clientId).addFetch(topic, partition, offset, 1024 * 1024 * 1).build();
				FetchResponse fetchResponse = consumer.fetch(req);

				// 错误判断
				if (fetchResponse.hasError()) {
					num_errors++;

					short code = fetchResponse.errorCode(topic, partition);
					LOGGER.error("#获取数据失败 from the Broker:" + leaderBroker + " Reason: " + code);
					if (code == ErrorMapping.OffsetOutOfRangeCode()) {
						LOGGER.error("#OffsetOutOfRangeCode");
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
								mapper.mapper(topic, partition, messageAndOffset.offset(), new String(bytes, "UTF-8"), infogen_kafkalzooutputformat);
							} catch (UnsupportedEncodingException e) {
								LOGGER.error("#数据转换失败:", e);
							}

							fetch_size += limit;
							offset = messageAndOffset.nextOffset();
						}
					}
					// END 遍历MESSAGE

					// 10分钟或64M
					if (fetch_size >= 1024 * 1024 * 64 || (Clock.systemUTC().millis() - millis) >= 1000 * 60 * 10) {
						try {
							infogen_kafkalzooutputformat.close_all();
						} catch (IOException e) {
							LOGGER.error("#关闭流失败-重试:", e);
							return;
						}
						fetch_size = 0l;
						millis = Clock.systemUTC().millis();
					}

					// 更新offset
					for (;;) {
						Stat set_data = infogen_zookeeper.set_data(InfoGen_ZooKeeper.offset(topic, partition), offset.toString().getBytes(), -1);
						if (set_data != null) {
							commit_offset = offset;
							infogen_kafkalzooutputformat.commit_offset = commit_offset;
							break;
						} else {
							try {
								Thread.sleep(1000);
							} catch (InterruptedException e) {
								LOGGER.error("", e);
							}
						}
					}
					// END 更新offset
				}
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
				LOGGER.error(host + "-" + port + "-" + topic, e);
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