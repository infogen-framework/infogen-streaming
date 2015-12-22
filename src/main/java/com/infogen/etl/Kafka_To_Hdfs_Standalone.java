package com.infogen.etl;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.infogen.mapper.InfoGen_Mapper;
import com.infogen.yarn.InfoGen_Container;
import com.infogen.yarn.Job_Configuration;

/**
 * 独立部署示例程序,可以直接传入参数启动
 * 
 * @author larry/larrylv@outlook.com/创建时间 2015年12月21日 下午1:12:07
 * @since 1.0
 * @version 1.0
 */
public class Kafka_To_Hdfs_Standalone {
	private static final Log LOGGER = LogFactory.getLog(Kafka_To_Hdfs_Standalone.class);

	public static void main(String[] args) throws InterruptedException, ClassNotFoundException, ParseException {
		Job_Configuration job_configuration = get_configuration(args);
		job_configuration.zookeeper = "172.16.8.97:2181,172.16.8.98:2181,172.16.8.99:2181";
		job_configuration.topic = "infogen_topic_tracking";
		job_configuration.group = "infogen_etl";
		@SuppressWarnings("unchecked")
		Class<? extends InfoGen_Mapper> mapper_clazz = (Class<? extends InfoGen_Mapper>) Class.forName("com.infogen.etl.Kafka_To_Hdfs_Mapper");
		job_configuration.mapper_clazz = mapper_clazz;
		job_configuration.parameters = "hdfs://spark101:8020/infogen/output/";
		job_configuration.numContainers = 5;
		if (job_configuration.zookeeper == null || job_configuration.topic == null || job_configuration.group == null || job_configuration.mapper_clazz == null) {
			LOGGER.error("#没有设置参数");
			printUsage();
			return;
		}

		for (int i = 0; i < job_configuration.numContainers; i++) {
			new Thread(() -> {
				try {
					InfoGen_Container infogen_container = new InfoGen_Container();
					infogen_container.run(job_configuration.zookeeper, job_configuration.topic, job_configuration.group, job_configuration.mapper_clazz, job_configuration.parameters);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}).start();
		}
		Thread.currentThread().join();
	}

	// Command line options
	private static Options opts = builder_client();

	public static Job_Configuration get_configuration(String[] args) throws ParseException, ClassNotFoundException {
		// "hdfs://spark101:8020/infogen/output/"
		Job_Configuration job_configuration = new Job_Configuration();
		LOGGER.info("#初始化Client配置");
		if (args.length == 0) {
			return job_configuration;
		}

		CommandLine cliParser = new GnuParser().parse(opts, args);
		if (cliParser.hasOption("help")) {
			printUsage();
		}

		job_configuration.zookeeper = cliParser.getOptionValue("zookeeper", null);
		job_configuration.topic = cliParser.getOptionValue("topic", null);
		job_configuration.group = cliParser.getOptionValue("group", null);
		@SuppressWarnings("unchecked")
		Class<? extends InfoGen_Mapper> mapper_clazz = (Class<? extends InfoGen_Mapper>) Class.forName(cliParser.getOptionValue("mapper_clazz", null));
		job_configuration.mapper_clazz = mapper_clazz;
		job_configuration.parameters = cliParser.getOptionValue("parameters", "");

		job_configuration.numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
		return job_configuration;
	}

	public static Options builder_client() {
		Options opts = new Options();
		opts.addOption("num_containers", true, "No. of containers on which the command needs to be executed");
		opts.addOption("debug", false, "Dump out debug information");
		opts.addOption("help", false, "Print usage");
		//
		opts.addOption("zookeeper", true, "zookeeper");
		opts.addOption("topic", true, "topic");
		opts.addOption("group", true, "group");
		opts.addOption("mapper_clazz", true, "mapper_clazz");
		opts.addOption("parameters", true, "parameters");
		return opts;
	}

	/**
	 * Helper function to print out usage
	 */
	private static void printUsage() {
		new HelpFormatter().printHelp("Client", opts);
	}
}
