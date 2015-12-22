package com.infogen.yarn;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;

import com.infogen.exception.NoPartition_Exception;
import com.infogen.kafka.InfoGen_Consumer;
import com.infogen.kafka.InfoGen_Consumer.AUTO_OFFSET_RESET;
import com.infogen.mapper.InfoGen_Mapper;
import com.infogen.zookeeper.InfoGen_ZooKeeper;

/**
 * ETL的Worker 会随机选择一个partition进行ETL，如果没有可用的partition则退出程序
 * 
 * @author larry/larrylv@outlook.com/创建时间 2015年12月4日 下午1:07:22
 * @since 1.0
 * @version 1.0
 */
public class InfoGen_Container {
	private static final Log LOGGER = LogFactory.getLog(InfoGen_Container.class);
	private static final Long MEGABYTE = 1024L * 1024L;

	public static Long bytesToMegabytes(Long bytes) {
		return bytes / MEGABYTE;
	}

	private Job_Configuration job_configuration;

	public InfoGen_Container(Job_Configuration job_configuration) {
		this.job_configuration = job_configuration;
	}

	public void submit() throws IOException {
		String zookeeper = job_configuration.zookeeper;
		String topic = job_configuration.topic;
		String group = job_configuration.group;
		String output = job_configuration.output;
		Class<? extends InfoGen_Mapper> mapper_clazz = job_configuration.mapper;
		String parameters = job_configuration.parameters;

		if (zookeeper == null || topic == null || group == null || mapper_clazz == null || output == null) {
			LOGGER.error("#没有设置参数zookeeper,topic,group,mapper,output");
			Job_Configuration.printUsage();
			return;
		}

		LOGGER.error("#InfoGen_Container启动");
		Long freeMemory = bytesToMegabytes(Runtime.getRuntime().freeMemory());
		Long totalMemory = bytesToMegabytes(Runtime.getRuntime().totalMemory());
		Long maxMemory = bytesToMegabytes(Runtime.getRuntime().maxMemory());
		LOGGER.error("################################################################");
		LOGGER.error("# The amount of free memory in the JVM:      " + freeMemory + "MB");
		LOGGER.error("# The total amount of memory in the JVM:     " + totalMemory + "MB");
		LOGGER.error("# The maximum amount of memory that the JVM: " + maxMemory + "MB");
		LOGGER.error("################################################################");

		InfoGen_ZooKeeper infogen_zookeeper = new InfoGen_ZooKeeper();
		infogen_zookeeper.start_zookeeper(zookeeper, null);
		infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.CONTEXT, CreateMode.PERSISTENT);
		infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.topic(topic), CreateMode.PERSISTENT);
		infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.topic(topic, group), CreateMode.PERSISTENT);
		infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.offset(topic, group), CreateMode.PERSISTENT);
		infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.partition(topic, group), CreateMode.PERSISTENT);
		infogen_zookeeper.stop_zookeeper();

		Long start_offset = null;
		for (;;) {
			try {
				start_offset = new InfoGen_Consumer().start(zookeeper, topic, group, start_offset, AUTO_OFFSET_RESET.smallest.name(), mapper_clazz, output, parameters);
			} catch (NoPartition_Exception e) {
				LOGGER.info("#没有获取到partition，退出ETL", e);
				return;
			} catch (IllegalAccessException | InstantiationException e) {
				LOGGER.error("#实例化InfoGen_Mapper失败，退出ETL", e);
				return;
			} catch (IOException e) {
				// zookeeper启动失败,session过期等引起的异常
				LOGGER.error("#zookeeper启动失败,session过期等引起的异常", e);
			}

			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				LOGGER.error("", e);
			}
		}
	}

	public static void main(String[] args) throws IOException, ParseException, ClassNotFoundException {
		Job_Configuration job_configuration = Job_Configuration.get_configuration(args);
		InfoGen_Container infogen_container = new InfoGen_Container(job_configuration);
		infogen_container.submit();
	}
}
