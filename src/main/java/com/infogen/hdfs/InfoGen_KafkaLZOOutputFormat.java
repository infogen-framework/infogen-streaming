package com.infogen.hdfs;

import java.io.IOException;
import java.util.concurrent.DelayQueue;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import com.infogen.core.structure.LRULinkedHashMap;
import com.infogen.zookeeper.InfoGen_ZooKeeper;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月15日 上午11:16:19
 * @since 1.0
 * @version 1.0
 */
public class InfoGen_KafkaLZOOutputFormat implements InfoGen_OutputFormat {
	private static Logger LOGGER = Logger.getLogger(InfoGen_KafkaLZOOutputFormat.class);
	private DelayQueue<InfoGen_LZOOutputStream> delayQueue = new DelayQueue<>();
	private LRULinkedHashMap<String, InfoGen_LZOOutputStream> map = new LRULinkedHashMap<>(10000);
	private InfoGen_ZooKeeper zookeeper = InfoGen_ZooKeeper.getInstance();
	private Long last_offset = 0l;
	private String topic;
	private Integer partition;

	public InfoGen_KafkaLZOOutputFormat(String topic, Integer partition) {
		this.topic = topic;
		this.partition = partition;
		delay();
	}

	public void write_line(StringBuilder dir_stringbuilder, String topic, Integer partition, Long offset, String message) {
		last_offset = offset;
		Integer num_errors = 0;
		Integer max_errors = 5;

		String dir = dir_stringbuilder.toString();
		String path = dir_stringbuilder.append(partition).append(".").append(offset).append("-").toString();

		InfoGen_LZOOutputStream infogen_hdfs_lzooutputstream = map.get(dir);
		for (;;) {// 尾递归优化代码可读性差，用循环代替
			try {
				if (infogen_hdfs_lzooutputstream != null && infogen_hdfs_lzooutputstream.write_line(message)) {
					return;
				} else {
					infogen_hdfs_lzooutputstream = new InfoGen_LZOOutputStream(new Path(path));
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
						LOGGER.error("#重试超过5次,打开新的流来重试写入");
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
					Stat set_data = zookeeper.set_data(InfoGen_ZooKeeper.offset(topic, partition), last_offset.toString().getBytes(), -1);
					if (set_data == null) {
						InfoGen_ZooKeeper.alive = false;
					}
					InfoGen_LZOOutputStream lzoOutputStream = delayQueue.take();
					lzoOutputStream.close(last_offset.toString() + ".finish");
				} catch (InterruptedException | IOException e) {
					LOGGER.error("#delayQueue.take()失败:", e);
				}
			}
		}).start();
	}

}
