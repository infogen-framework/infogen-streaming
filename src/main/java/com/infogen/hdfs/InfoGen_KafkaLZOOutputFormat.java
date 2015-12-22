package com.infogen.hdfs;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * 针对kafka实现使用InfoGen_KafkaLZOOutputStream写入数据的OutputFormat
 * @author larry/larrylv@outlook.com/创建时间 2015年12月15日 上午11:16:19
 * @since 1.0
 * @version 1.0
 */
public class InfoGen_KafkaLZOOutputFormat implements InfoGen_OutputFormat {
	private static Logger LOGGER = Logger.getLogger(InfoGen_KafkaLZOOutputFormat.class);
	private ConcurrentHashMap<String, InfoGen_KafkaLZOOutputStream> map = new ConcurrentHashMap<>(10000);
	private Long commit_offset;
	private Integer partition;

	public InfoGen_KafkaLZOOutputFormat(Integer partition) {
		this.partition = partition;
	}

	public void setCommit_offset(Long commit_offset) {
		this.commit_offset = commit_offset;
	}

	public void write_line(String path, String message) throws IllegalArgumentException, IOException {
		Integer num_errors = 0;
		Integer max_errors = 5;

		String full_path = new StringBuilder(path).append("-").append(partition).append(".").append(commit_offset).toString();

		InfoGen_KafkaLZOOutputStream infogen_hdfs_lzooutputstream = map.get(path);
		for (;;) {// 尾递归优化代码可读性差，用循环代替
			try {
				if (infogen_hdfs_lzooutputstream != null && infogen_hdfs_lzooutputstream.write_line(message)) {
					break;
				} else {
					infogen_hdfs_lzooutputstream = new InfoGen_KafkaLZOOutputStream(new Path(full_path));
					map.put(path, infogen_hdfs_lzooutputstream);
				}
			} catch (IllegalArgumentException | IOException e) {
				LOGGER.error("#写入hdfs失败:", e);
				num_errors++;
				if (num_errors > max_errors) {
					throw e;
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

	public Integer number_io() {
		return map.size();
	}

	public Boolean close_all() {
		Boolean flag = true;
		for (Entry<String, InfoGen_KafkaLZOOutputStream> entry : map.entrySet()) {
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
