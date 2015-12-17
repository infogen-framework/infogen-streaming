package com.infogen.hdfs;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月15日 上午11:16:19
 * @since 1.0
 * @version 1.0
 */
public class InfoGen_KafkaLZOOutputFormat implements InfoGen_OutputFormat {
	private static Logger LOGGER = Logger.getLogger(InfoGen_KafkaLZOOutputFormat.class);

	private static class InnerInstance {
		public static final InfoGen_OutputFormat instance = new InfoGen_KafkaLZOOutputFormat();
	}

	public static InfoGen_OutputFormat getInstance() {
		return InnerInstance.instance;
	}

	private InfoGen_KafkaLZOOutputFormat() {
	}

	private ConcurrentHashMap<String, InfoGen_LZOOutputStream> map = new ConcurrentHashMap<>(10000);

	public void write_line(String path, String topic, Integer partition, Long offset, String message) {
		Integer num_errors = 0;
		Integer max_errors = 5;

		String full_path = new StringBuilder(path).append("-").append(partition).append(".").append(offset).toString();

		InfoGen_LZOOutputStream infogen_hdfs_lzooutputstream = map.get(path);
		for (;;) {// 尾递归优化代码可读性差，用循环代替
			try {
				if (infogen_hdfs_lzooutputstream != null && infogen_hdfs_lzooutputstream.write_line(message)) {
					break;
				} else {
					infogen_hdfs_lzooutputstream = new InfoGen_LZOOutputStream(new Path(full_path));
					map.put(path, infogen_hdfs_lzooutputstream);
				}
			} catch (IllegalArgumentException | IOException e) {
				LOGGER.error("#写入hdfs失败:", e);
				num_errors++;
				if (num_errors > max_errors) {
					try {
						num_errors = 0;
						if (infogen_hdfs_lzooutputstream != null) {
							infogen_hdfs_lzooutputstream.close();
						}
						LOGGER.error("#重试超过5次,打开新的流来重试写入");
					} catch (IOException e2) {
						LOGGER.error("#关闭流失败", e2);
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

	public Boolean close_all() {
		Boolean flag = true;
		for (Entry<String, InfoGen_LZOOutputStream> entry : map.entrySet()) {
			map.remove(entry.getKey());
			try {
				entry.getValue().close();
			} catch (IOException e) {
				flag = false;
				LOGGER.error("#关闭流失败", e);
			}
		}
		return flag;
	}
}
