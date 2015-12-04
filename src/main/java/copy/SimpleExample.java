package copy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class SimpleExample {
	private List<String> m_replicaBrokers = new ArrayList<String>();

	public SimpleExample() {
		m_replicaBrokers = new ArrayList<String>();
	}

	public static void main(String args[]) {
		SimpleExample example = new SimpleExample();
		try {
			example.run(Long.parseLong("3"), "infogen_topic_tracking", Integer.parseInt("0"), "172.16.8.97,172.16.8.98,172.16.8.99", Integer.parseInt("10086"));
		} catch (Exception e) {
			System.out.println("Oops:" + e);
			e.printStackTrace();
		}
	}

	// 找到指定分区的元数据
	private PartitionMetadata findPartitionMetadata(String brokerHosts, Integer port, String topic, int partition) {
		PartitionMetadata returnMetaData = null;
		for (String brokerHost : brokerHosts.split(",")) {
			SimpleConsumer consumer = null;
			consumer = new SimpleConsumer(brokerHost, port, 100000, 64 * 1024, "leaderLookup");
			List<String> topics = Collections.singletonList(topic);
			TopicMetadataRequest request = new TopicMetadataRequest(topics);
			TopicMetadataResponse response = consumer.send(request);
			List<TopicMetadata> topicMetadatas = response.topicsMetadata();
			for (TopicMetadata topicMetadata : topicMetadatas) {
				for (PartitionMetadata PartitionMetadata : topicMetadata.partitionsMetadata()) {
					if (PartitionMetadata.partitionId() == partition) {
						returnMetaData = PartitionMetadata;
					}
				}
			}
			if (consumer != null)
				consumer.close();
		}
		return returnMetaData;
	}

	public void run(long maxReads, String topic, int partition, String brokers, int port) throws Exception {
		// 获取指定Topic partition的元数据
		PartitionMetadata metadata = findPartitionMetadata(brokers, port, topic, partition);
		if (metadata == null) {
			System.out.println("Can't find metadata for Topic and Partition. Exiting");
			return;
		}
		if (metadata.leader() == null) {
			System.out.println("Can't find Leader for Topic and Partition. Exiting");
			return;
		}
		String leadBroker = metadata.leader().host();
		String clientName = "Client_" + topic + "_" + partition;

		SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
		long readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
		int numErrors = 0;
		while (maxReads > 0) {
			FetchRequest req = new FetchRequestBuilder().clientId(clientName).addFetch(topic, partition, readOffset, 100000).build();
			FetchResponse fetchResponse = consumer.fetch(req);
			// 错误判断
			if (fetchResponse.hasError()) {
				short code = fetchResponse.errorCode(topic, partition);
				System.out.println("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
					readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
					continue;
				} else if (numErrors > 5) {
					break;
				}
				numErrors++;
			} else {
				numErrors = 0;
			}

			Boolean has = false;
			for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
				long currentOffset = messageAndOffset.offset();
				if (currentOffset < readOffset) {
					System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
					continue;
				}
				readOffset = messageAndOffset.nextOffset();
				ByteBuffer payload = messageAndOffset.message().payload();

				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
				has = true;
				maxReads--;
			}

			if (has) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}
		if (consumer != null)
			consumer.close();
	}

	public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		if (response.hasError()) {
			System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}

}