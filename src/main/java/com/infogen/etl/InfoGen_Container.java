package com.infogen.etl;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
	public Class<? extends InfoGen_Mapper> mapper_clazz;
	private static Options opts = builder_applicationmaster();

	public static void main(String[] args) throws IOException, ParseException {
		CommandLine cliParser = new GnuParser().parse(opts, args);
		if (cliParser.hasOption("help")) {
			printUsage(opts);
			System.exit(0);
		}

		// String topic = cliParser.getOptionValue("topic");
		// String zookeeper = cliParser.getOptionValue("zookeeper");
		String topic = "infogen_topic_tracking";
		String zookeeper = "172.16.8.97:2181,172.16.8.98:2181,172.16.8.99:2181";
		if (topic == null || zookeeper == null) {
			printUsage(opts);
			System.exit(0);
		}

		InfoGen_ZooKeeper infogen_zookeeper = InfoGen_ZooKeeper.getInstance();
		infogen_zookeeper.start_zookeeper(zookeeper);

		infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.CONTEXT, CreateMode.PERSISTENT);
		infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.topic(topic), CreateMode.PERSISTENT);
		infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.offset(topic), CreateMode.PERSISTENT);
		infogen_zookeeper.create_notexists(InfoGen_ZooKeeper.partition(topic), CreateMode.PERSISTENT);

		Integer partition = -1;
		Integer partitions_size = infogen_zookeeper.get_childrens("/brokers/topics/" + topic + "/partitions").size();

		for (;;) {
			for (int i = 0; i < partitions_size; i++) {
				String create = infogen_zookeeper.create(InfoGen_ZooKeeper.partition(topic, i), null, CreateMode.EPHEMERAL);
				if (create == null || create.equals(Code.NODEEXISTS.name())) {
					LOGGER.info("#创建partition失败:" + i);
				} else {
					LOGGER.info("#创建partition成功:" + i);
					partition = i;
					Kafka_To_Hdfs_Mapper kafka_to_hdfs = new Kafka_To_Hdfs_Mapper(topic, partition);
					InfoGen_Consumer consumer = new InfoGen_Consumer(topic, partition);
					consumer.run(kafka_to_hdfs);
				}
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				LOGGER.error("", e);
			}
		}
	}

	public static Options builder_applicationmaster() {
		Options opts = new Options();
		opts.addOption("help", false, "Print usage");
		opts.addOption("mapper_clazz", true, "mapper_clazz");
		opts.addOption("topic", true, "topic");
		opts.addOption("zookeeper", true, "zookeeper");
		return opts;
	}

	private static void printUsage(Options opts) {
		new HelpFormatter().printHelp("InfoGen_Container", opts);
	}
}
