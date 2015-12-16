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
import com.infogen.mapper.InfoGen_Mapper;
import com.infogen.zookeeper.InfoGen_ZooKeeper;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月4日 下午1:07:22
 * @since 1.0
 * @version 1.0
 */
public class InfoGen_Container {
	private static final Log LOGGER = LogFactory.getLog(InfoGen_Container.class);

	private void run(String topic, String zookeeper, String mapper_clazz) throws ClassNotFoundException, IOException {
		@SuppressWarnings("unchecked")
		Class<? extends InfoGen_Mapper> infogen_mapper_class = (Class<? extends InfoGen_Mapper>) Class.forName(mapper_clazz);

		InfoGen_ZooKeeper infogen_zookeeper = new InfoGen_ZooKeeper();
		infogen_zookeeper.start_zookeeper(zookeeper, null);
		infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.CONTEXT, CreateMode.PERSISTENT);
		infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.topic(topic), CreateMode.PERSISTENT);
		infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.offset(topic), CreateMode.PERSISTENT);
		infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.partition(topic), CreateMode.PERSISTENT);

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

		// partition
		Integer partition = -1;
		Integer partitions_size = infogen_zookeeper.get_childrens("/brokers/topics/" + topic + "/partitions").size();
		for (int i = 0; i < partitions_size; i++) {
			infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.offset(topic, i), CreateMode.PERSISTENT);
			String create = infogen_zookeeper.create(InfoGen_ZooKeeper.partition(topic, i), null, CreateMode.EPHEMERAL);
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

		// offset
		Long zookeeper_offset = null;
		String get_offset = infogen_zookeeper.get_data(InfoGen_ZooKeeper.offset(topic, partition));
		if (get_offset != null) {
			LOGGER.info("#使用zookeeper 中 offset为:" + zookeeper_offset);
			zookeeper_offset = Long.valueOf(get_offset);
		}
		//
		infogen_zookeeper.stop_zookeeper();

		// TODO 使用传入的offset还是zookeeper中的offset
		Long commit_offset = zookeeper_offset;
		for (;;) {
			try {
				InfoGen_Mapper infogen_mapper = infogen_mapper_class.newInstance();
				commit_offset = new InfoGen_Consumer().start(zookeeper, brokers, topic, partition, commit_offset, infogen_mapper, "largest");
				// consumer错误次数超上限等原因会退出
			} catch (Exception e) {
				// zookeeper启动失败,session过期等引起的异常
				LOGGER.error("", e);
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
		CommandLine cliParser = new GnuParser().parse(opts, args);

		// String topic = cliParser.getOptionValue("topic");
		// String zookeeper = cliParser.getOptionValue("zookeeper");
		String topic = "infogen_topic_tracking";
		String zookeeper = "172.16.8.97:2181,172.16.8.98:2181,172.16.8.99:2181";
		String mapper_clazz = "com.infogen.etl.Kafka_To_Hdfs_Mapper";
		if (topic == null || zookeeper == null || mapper_clazz == null) {
			LOGGER.error("参数不能为空");
			printUsage(opts);
			System.exit(0);
		}

		InfoGen_Container infogen_container = new InfoGen_Container();
		infogen_container.run(topic, zookeeper, mapper_clazz);
	}

	public static Options builder_applicationmaster() {
		Options opts = new Options();
		opts.addOption("mapper_clazz", true, "mapper_clazz");
		opts.addOption("topic", true, "topic");
		opts.addOption("zookeeper", true, "zookeeper");
		return opts;
	}

	private static void printUsage(Options opts) {
		new HelpFormatter().printHelp("InfoGen_Container", opts);
	}
}
