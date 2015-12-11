package com.infogen.kafka;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

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

	@SuppressWarnings("static-access")
	public static void main(String args[]) {
		InfoGen_Consumer example = new InfoGen_Consumer("172.16.8.97,172.16.8.98,172.16.8.99", 10086, "test_simple_example", 2);
		for (;;) {
			try {
				example.run(0l, (topic, partition, offset, message) -> {
					System.out.println(topic + "-" + partition + "-" + offset + "-" + message);
				});
			} catch (Exception e) {
				LOGGER.error("", e);
			}

			try {
				Thread.currentThread().sleep(1000);
			} catch (InterruptedException e) {
				LOGGER.error("", e);
			}
		}
	}

	private String brokers;
	private Integer port;
	private String topic;
	private Integer partition;

	public InfoGen_Consumer(String brokers, Integer port, String topic, int partition) {
		this.brokers = brokers;
		this.port = port;
		this.topic = topic;
		this.partition = partition;
	}

	// 找到指定分区的元数据
	private PartitionMetadata findPartitionMetadata() {
		PartitionMetadata return_partition_metadata = null;
		List<String> topics = Collections.singletonList(topic);
		for (String broker : brokers.split(",")) {
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(broker, port, 100000, 64 * 1024, "partitionLookup");
				TopicMetadataRequest request = new TopicMetadataRequest(topics);
				TopicMetadataResponse response = consumer.send(request);

				for (TopicMetadata topicMetadata : response.topicsMetadata()) {
					for (PartitionMetadata partition_metadata : topicMetadata.partitionsMetadata()) {
						if (partition_metadata.partitionId() == partition) {
							return_partition_metadata = partition_metadata;
						}
					}
				}
			} catch (Exception e) {
				LOGGER.error(broker + "-" + port + "-" + topic, e);
			} finally {
				if (consumer != null) {
					consumer.close();
				}
			}
		}
		return return_partition_metadata;
	}

	public void run(Long offset, Message_Handle handle) throws Exception {
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
		String clientId = "Client_" + topic + "_" + partition + "_" + Clock.systemUTC().millis();

		SimpleConsumer consumer = null;
		try {
			consumer = new SimpleConsumer(leaderBroker, port, 100000, 64 * 1024, clientId);

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

			if (offset < earliestOffset) {
				LOGGER.warn("#offset不存在-从最早的offset开始获取:" + earliestOffset);
				offset = earliestOffset;
			} else if (offset > latestOffset) {
				LOGGER.warn("#offset不存在-从最后的offset开始获取:" + latestOffset);
				offset = latestOffset;
			} else {

			}
			System.out.println(latestOffset);
			// offset = latestOffset;
			Integer num_errors = 0;
			Integer max_errors = 3;
			for (;;) {
				// 读取大小为100000 超过会返回一个空的fetchResponse
				FetchRequest req = new FetchRequestBuilder().clientId(clientId).addFetch(topic, partition, offset, 100000).build();
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
				} else {
					num_errors = 0;
					Boolean need_sleep = true;
					for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
						long currentOffset = messageAndOffset.offset();
						if (currentOffset < offset) {
							LOGGER.info("#Found an old offset: " + currentOffset + " Expecting: " + offset);
						} else {
							offset = messageAndOffset.nextOffset();
							ByteBuffer payload = messageAndOffset.message().payload();
							byte[] bytes = new byte[payload.limit()];
							payload.get(bytes);
							handle.handle(topic, partition, messageAndOffset.offset(), new String(bytes, "UTF-8"));
						}
						need_sleep = false;
					}

					if (need_sleep) {
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							LOGGER.error("", e);
						}
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error("", e);
		} finally {
			if (consumer != null) {
				consumer.close();
			}
		}
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