package com.infogen.etl.kafka_to_hdfs;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import com.infogen.hdfs.InfoGen_Hdfs_LZOOutputStream;
import com.infogen.kafka.InfoGen_Consumer;
import com.infogen.kafka.Message_Handle;
import com.infogen.zookeeper.InfoGen_ZooKeeper;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月10日 下午2:15:29
 * @since 1.0
 * @version 1.0
 */
public class Kafka_To_Hdfs implements Message_Handle {
	private static Logger LOGGER = Logger.getLogger(Kafka_To_Hdfs.class);
	private DelayQueue<InfoGen_Hdfs_LZOOutputStream> delayQueue = new DelayQueue<>();
	private ConcurrentHashMap<String, InfoGen_Hdfs_LZOOutputStream> map = new ConcurrentHashMap<>();
	private InfoGen_ZooKeeper zookeeper = InfoGen_ZooKeeper.getInstance();
	final ZoneId zoneidPlus8 = ZoneId.of("UTC+8"); // UTC時間+8
	private Long last_offset = 0l;
	private String topic;
	private Integer partition;

	public Kafka_To_Hdfs(String topic, Integer partition) {
		this.topic = topic;
		this.partition = partition;
	}

	public String mapper(String topic, Integer partition, Long offset, String message) {
		return "";
	}

	@Override
	public void handle(String topic, Integer partition, Long offset, String message) {
		last_offset = offset;
		Integer num_errors = 0;
		Integer max_errors = 5;
		String[] split = message.split(",");
		if (split.length < 9) {
			return;
		}

		LocalDateTime localdatetime = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.valueOf(split[9])), zoneidPlus8);
		StringBuilder sbd = new StringBuilder("hdfs://spark101:8020/infogen/output/").append(topic).append("/");
		sbd.append(localdatetime.getYear()).append("-").append(localdatetime.getMonthValue()).append("-").append(localdatetime.getDayOfMonth()).append("/");
		sbd.append(localdatetime.getHour()).append("/").append(localdatetime.getMinute()).append("/");
		String dir = sbd.toString();
		String path = sbd.append(partition).append(".").append(offset).append("-").toString();

		InfoGen_Hdfs_LZOOutputStream infogen_hdfs_lzooutputstream = map.get(dir);
		for (;;) {// 尾递归优化代码可读性差，用循环代替
			try {
				if (infogen_hdfs_lzooutputstream == null) {
					infogen_hdfs_lzooutputstream = new InfoGen_Hdfs_LZOOutputStream(new Path(path));
					delayQueue.put(infogen_hdfs_lzooutputstream);
					map.put(path, infogen_hdfs_lzooutputstream);
				}

				if (infogen_hdfs_lzooutputstream.write_line(message)) {
					return;
				} else {
					infogen_hdfs_lzooutputstream = new InfoGen_Hdfs_LZOOutputStream(new Path(path));
					delayQueue.put(infogen_hdfs_lzooutputstream);
					map.put(path, infogen_hdfs_lzooutputstream);
				}
			} catch (IllegalArgumentException | IOException e) {
				LOGGER.error("#写入hdfs失败:", e);
				num_errors++;
				if (num_errors > max_errors) {
					try {
						num_errors = 0;
						if (infogen_hdfs_lzooutputstream != null) {
							infogen_hdfs_lzooutputstream.close(offset.toString());
						}
					} catch (IOException e1) {
						LOGGER.error("#关闭流失败", e1);
					}
				} else {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						LOGGER.error("", e1);
					}
				}
			}
		}
	}

	private void delay() {
		new Thread(() -> {
			for (;;) {
				try {
					InfoGen_Hdfs_LZOOutputStream lzoOutputStream = delayQueue.take();
					lzoOutputStream.close(last_offset.toString() + ".finish");
					zookeeper.set_data(InfoGen_ZooKeeper.path(topic, partition.toString()), last_offset.toString().getBytes(), -1);
					map.remove(lzoOutputStream.getPath().toString());
				} catch (InterruptedException | IOException e) {
					LOGGER.error("#delayQueue.take()失败:", e);
				}
			}
		}).start();
	}

	public static void main(String[] args) throws IOException {
		String zookeeper_hosts = "172.16.8.97:2181,172.16.8.98:2181,172.16.8.99:2181";
		String topic = "infogen_topic_tracking";
		Integer partition = 1;
		Long offset = 0l;
		Long end_offset = Long.MAX_VALUE;

		InfoGen_ZooKeeper zookeeper = InfoGen_ZooKeeper.getInstance();
		zookeeper.start_zookeeper(zookeeper_hosts);
		// 为减少jar包的大小，未使用json
		StringBuilder brokers_sb = new StringBuilder();
		for (String id : zookeeper.get_childrens("/brokers/ids")) {
			for (String kv : zookeeper.get_data("/brokers/ids/" + id).replace("{", "").replace("}", "").replace("\"", "").split(",")) {
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
		String brokers = brokers_sb.substring(0, brokers_sb.length() - 1);

		if (zookeeper.exists(InfoGen_ZooKeeper.CONTEXT) == null) {
			zookeeper.create(InfoGen_ZooKeeper.CONTEXT, null, CreateMode.PERSISTENT);
		}
		if (zookeeper.exists(InfoGen_ZooKeeper.path(topic)) == null) {
			zookeeper.create(InfoGen_ZooKeeper.path(topic), null, CreateMode.PERSISTENT);
		}
		if (zookeeper.exists(InfoGen_ZooKeeper.path(topic, partition.toString())) == null) {
			zookeeper.create(InfoGen_ZooKeeper.path(topic, partition.toString()), null, CreateMode.PERSISTENT);
		}
		String get_data = zookeeper.get_data(InfoGen_ZooKeeper.path(topic, partition.toString()));
		if (get_data != null) {
			LOGGER.info("#使用zookeeper 中 offset为:" + offset);
			offset = Long.valueOf(get_data);
		} else {
			zookeeper.set_data(InfoGen_ZooKeeper.path(topic, partition.toString()), offset.toString().getBytes(), -1);
			LOGGER.info("#使用默认 offset为:" + offset);
		}

		Kafka_To_Hdfs kafka_to_hdfs = new Kafka_To_Hdfs(topic, partition);
		kafka_to_hdfs.delay();
		InfoGen_Consumer consumer = new InfoGen_Consumer(brokers, topic, partition, offset, end_offset);
		consumer.start(kafka_to_hdfs);
	}

}
