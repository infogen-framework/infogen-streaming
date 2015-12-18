package com.infogen.etl;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;

import com.infogen.kafka.InfoGen_Consumer;
import com.infogen.kafka.InfoGen_Consumer.AUTO_OFFSET_RESET;
import com.infogen.mapper.InfoGen_Mapper;
import com.infogen.zookeeper.InfoGen_ZooKeeper;

/**
 * 
 * @author larry/larrylv@outlook.com/创建时间 2015年12月4日 下午1:07:22
 * @since 1.0
 * @version 1.0
 */
public class InfoGen_Container {
	private static final Log LOGGER = LogFactory.getLog(InfoGen_Container.class);
	private static final long MEGABYTE = 1024L * 1024L;

	public static long bytesToMegabytes(long bytes) {
		return bytes / MEGABYTE;
	}

	private void run(String zookeeper, String topic, String group, String mapper_clazz) throws ClassNotFoundException, IOException {
		@SuppressWarnings("unchecked")
		Class<? extends InfoGen_Mapper> infogen_mapper_class = (Class<? extends InfoGen_Mapper>) Class.forName(mapper_clazz);

		InfoGen_ZooKeeper infogen_zookeeper = new InfoGen_ZooKeeper();
		infogen_zookeeper.start_zookeeper(zookeeper, null);
		infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.CONTEXT, CreateMode.PERSISTENT);
		infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.topic(topic), CreateMode.PERSISTENT);
		infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.topic(topic, group), CreateMode.PERSISTENT);
		infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.offset(topic, group), CreateMode.PERSISTENT);
		infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.partition(topic, group), CreateMode.PERSISTENT);

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
		String brokers = brokers_sb.substring(0, brokers_sb.length() - 1);
		LOGGER.info("#broker为：" + brokers);

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
			return;
		}
		LOGGER.info("#partition为：" + partition);

		// offset
		Long zookeeper_offset = null;
		String get_offset = infogen_zookeeper.get_data(InfoGen_ZooKeeper.offset(topic, group, partition));
		if (get_offset != null) {
			LOGGER.info("#使用zookeeper 中 offset为:" + zookeeper_offset);
			zookeeper_offset = Long.valueOf(get_offset);
		}
		LOGGER.info("#zookeeper_offset为：" + zookeeper_offset);

		// 关闭 zookeeper
		infogen_zookeeper.stop_zookeeper();

		Long commit_offset = zookeeper_offset;
		for (;;) {
			try {
				LOGGER.info("#执行ETL commit_offset为：" + commit_offset);
				InfoGen_Mapper infogen_mapper = infogen_mapper_class.newInstance();
				commit_offset = new InfoGen_Consumer().start(zookeeper, brokers, topic, group, partition, commit_offset, infogen_mapper, AUTO_OFFSET_RESET.smallest.name());
				// consumer错误次数超上限等原因会退出
				LOGGER.error("#退出执行ETL commit_offset为：" + commit_offset + " (consumer错误次数超上限等原因会退出)");
			} catch (Exception e) {
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

	///////////////////////////////////////////////////////////////////////////////
	private static Options opts = builder_applicationmaster();

	public static void main(String[] args) throws IOException, ParseException, ClassNotFoundException {
		LOGGER.error("#InfoGen_Container启动");
		long freeMemory = bytesToMegabytes(Runtime.getRuntime().freeMemory());
		long totalMemory = bytesToMegabytes(Runtime.getRuntime().totalMemory());
		long maxMemory = bytesToMegabytes(Runtime.getRuntime().maxMemory());

		LOGGER.error("################################################################");
		LOGGER.error("# The amount of free memory in the JVM:      " + freeMemory + "MB");
		LOGGER.error("# The total amount of memory in the JVM:     " + totalMemory + "MB");
		LOGGER.error("# The maximum amount of memory that the JVM: " + maxMemory + "MB");
		LOGGER.error("################################################################");

		CommandLine cliParser = new GnuParser().parse(opts, args);

		String topic = cliParser.getOptionValue("topic");
		String zookeeper = cliParser.getOptionValue("zookeeper");
		String group = cliParser.getOptionValue("group");
		String mapper_clazz = cliParser.getOptionValue("mapper_clazz");
		// String zookeeper = "172.16.8.97:2181,172.16.8.98:2181,172.16.8.99:2181";
		// String topic = "infogen_topic_tracking";
		// String group = "infogen_etl";
		// String mapper_clazz = "com.infogen.etl.Kafka_To_Hdfs_Mapper";
		if (topic == null || zookeeper == null || group == null || mapper_clazz == null) {
			LOGGER.error("参数不能为空");
			printUsage(opts);
			System.exit(0);
		}

		InfoGen_Container infogen_container = new InfoGen_Container();
		infogen_container.run(zookeeper, topic, group, mapper_clazz);
	}

	public static Options builder_applicationmaster() {
		Options opts = new Options();
		opts.addOption("zookeeper", true, "zookeeper");
		opts.addOption("topic", true, "topic");
		opts.addOption("group", true, "group");
		opts.addOption("mapper_clazz", true, "mapper_clazz");
		return opts;
	}

	private static void printUsage(Options opts) {
		new HelpFormatter().printHelp("InfoGen_Container", opts);
	}
}
