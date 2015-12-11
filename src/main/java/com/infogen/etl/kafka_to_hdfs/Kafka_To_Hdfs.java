package com.infogen.etl.kafka_to_hdfs;

import java.io.IOException;
import java.time.Clock;
import java.util.concurrent.DelayQueue;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.infogen.hdfs.InfoGen_Hdfs_LZOOutputStream;
import com.infogen.kafka.InfoGen_Consumer;
import com.infogen.kafka.Message_Handle;

import kafka.consumer.ConsumerIterator;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月10日 下午2:15:29
 * @since 1.0
 * @version 1.0
 */
public class Kafka_To_Hdfs {
	private static Logger LOGGER = Logger.getLogger(Kafka_To_Hdfs.class);
	private DelayQueue<InfoGen_Hdfs_LZOOutputStream> delayQueue = new DelayQueue<>();

	private Boolean has = false;

	// start end
	public Long update_time = Clock.systemUTC().millis();

	private final Integer interval = 60 * 1000;

	public static void main(String[] args) {
		InfoGen_Consumer example = new InfoGen_Consumer("172.16.8.97,172.16.8.98,172.16.8.99", 10086, "test_simple_example", 2);
		InfoGen_Hdfs_LZOOutputStream InfoGen_Hdfs_LZOOutputStream;
		try {
			InfoGen_Hdfs_LZOOutputStream = new InfoGen_Hdfs_LZOOutputStream(new Path("hdfs://spark101:8020/test/output/lzo/1.lzo"));
		} catch (IllegalArgumentException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		for (;;) {
			try {
				example.run(0l, (topic, partition, offset, message) -> {
					System.out.println(topic + "-" + partition + "-" + offset + "-" + message);
					InfoGen_Hdfs_LZOOutputStream.write_line(message, offset);
				});
			} catch (Exception e) {
				LOGGER.error("", e);
			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				LOGGER.error("", e);
			}
		}
	}
}
