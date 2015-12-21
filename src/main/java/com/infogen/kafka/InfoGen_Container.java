package com.infogen.kafka;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;

import com.infogen.exception.NoPartition_Exception;
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

	public void run(final String zookeeper, final String topic, final String group, final Class<? extends InfoGen_Mapper> infogen_mapper_class, final String parameters) throws ClassNotFoundException, IOException {
		LOGGER.error("#InfoGen_Container启动");
		long freeMemory = bytesToMegabytes(Runtime.getRuntime().freeMemory());
		long totalMemory = bytesToMegabytes(Runtime.getRuntime().totalMemory());
		long maxMemory = bytesToMegabytes(Runtime.getRuntime().maxMemory());

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

		Long commit_offset = null;
		for (;;) {
			try {
				LOGGER.info("#执行ETL commit_offset为：" + commit_offset);
				commit_offset = new InfoGen_Consumer().start(zookeeper, topic, group, commit_offset, AUTO_OFFSET_RESET.smallest.name(), infogen_mapper_class, parameters);
				// consumer错误次数超上限等原因会退出
				LOGGER.error("#退出执行ETL commit_offset为：" + commit_offset + " (consumer错误次数超上限等原因会退出)");
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

	///////////////////////////////////////////////////////////////////////////////
	private static Options opts = builder_applicationmaster();

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException, ParseException, ClassNotFoundException {
		CommandLine cliParser = new GnuParser().parse(opts, args);
		String zookeeper = cliParser.getOptionValue("zookeeper");
		String topic = cliParser.getOptionValue("topic");
		String group = cliParser.getOptionValue("group");
		String mapper_clazz = cliParser.getOptionValue("mapper_clazz");
		@SuppressWarnings("restriction")
		String parameters = new sun.misc.BASE64Decoder().decodeBuffer(cliParser.getOptionValue("parameters", "")).toString();
		if (topic == null || zookeeper == null || group == null || mapper_clazz == null) {
			LOGGER.error("参数不能为空");
			printUsage(opts);
			return;
		}

		InfoGen_Container infogen_container = new InfoGen_Container();
		infogen_container.run(zookeeper, topic, group, (Class<? extends InfoGen_Mapper>) Class.forName(mapper_clazz), parameters);
	}

	public static Options builder_applicationmaster() {
		Options opts = new Options();
		opts.addOption("zookeeper", true, "zookeeper");
		opts.addOption("topic", true, "topic");
		opts.addOption("group", true, "group");
		opts.addOption("mapper_clazz", true, "mapper_clazz");
		opts.addOption("parameters", true, "parameters");
		return opts;
	}

	private static void printUsage(Options opts) {
		new HelpFormatter().printHelp("InfoGen_Container", opts);
	}
}
