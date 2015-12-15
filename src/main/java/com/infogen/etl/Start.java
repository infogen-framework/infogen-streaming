package com.infogen.etl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Shell;

import com.infogen.yarn.InfoGen_Job;
import com.infogen.yarn.Job_Configuration;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月15日 下午2:13:55
 * @since 1.0
 * @version 1.0
 */
public class Start {
	private static final Log LOGGER = LogFactory.getLog(Start.class);

	public static void main(String[] args) throws ParseException {
		Job_Configuration job_configuration = get_configuration(args);
		InfoGen_Job infogen_job = new InfoGen_Job(job_configuration);

		infogen_job.run("infogen-etl-kafka", "infogen_topic_tracking", "172.16.8.97:2181,172.16.8.98:2181,172.16.8.99:2181", Kafka_To_Hdfs_Mapper.class);
	}

	// Command line options
	private static Options opts = builder_client();

	public static Job_Configuration get_configuration(String[] args) throws ParseException {
		Job_Configuration job_configuration = new Job_Configuration();
		LOGGER.info("#初始化Client配置");
		if (args.length == 0) {
			LOGGER.error("#没有设置参数");
			printUsage();
			return job_configuration;
		}

		CommandLine cliParser = new GnuParser().parse(opts, args);
		if (cliParser.hasOption("help")) {
			printUsage();
			System.exit(0);
		}
		if (cliParser.hasOption("debug")) {
			LOGGER.info("#启动debug");
			dumpOutDebugInfo();
			job_configuration.debugFlag = true;
		}

		job_configuration.user = cliParser.getOptionValue("user", System.getProperty("user.name"));
		job_configuration.amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
		job_configuration.amQueue = cliParser.getOptionValue("queue", "default");
		job_configuration.nodeLabelExpression = cliParser.getOptionValue("node_label_expression", null);
		if (cliParser.hasOption("keep_containers_across_application_attempts")) {
			LOGGER.info("keep_containers_across_application_attempts");
			job_configuration.keepContainers = true;
		}
		job_configuration.attemptFailuresValidityInterval = Long.parseLong(cliParser.getOptionValue("attempt_failures_validity_interval", "-1"));

		job_configuration.amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "512"));
		job_configuration.amVCores = Integer.parseInt(cliParser.getOptionValue("master_vcores", "1"));
		job_configuration.containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "512"));
		job_configuration.containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
		job_configuration.numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
		if (job_configuration.amMemory < 0) {
			throw new IllegalArgumentException("Invalid memory specified for application master, exiting. Specified memory=" + job_configuration.amMemory);
		}
		if (job_configuration.amVCores < 0) {
			throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting. Specified virtual cores=" + job_configuration.amVCores);
		}
		if (job_configuration.containerMemory < 0) {
			throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified, exiting. Specified containerMemory=" + job_configuration.containerMemory);
		}
		if (job_configuration.containerVirtualCores < 0) {
			throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified, exiting. Specified  containerVirtualCores=" + job_configuration.containerVirtualCores);
		}
		if (job_configuration.numContainers < 1) {
			throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified, exiting. Specified numContainer=" + job_configuration.numContainers);
		}
		return job_configuration;
	}

	public static Options builder_client() {
		Options opts = new Options();
		opts.addOption("priority", true, "Application Priority. Default 0");
		opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
		opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
		opts.addOption("master_vcores", true, "Amount of virtual cores to be requested to run the application master");
		opts.addOption("user", true, "执行用户");
		opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the command");
		opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run the command");
		opts.addOption("num_containers", true, "No. of containers on which the command needs to be executed");
		opts.addOption("keep_containers_across_application_attempts", false, "Flag to indicate whether to keep containers across application attempts. If the flag is true, running containers will not be killed when" + " application attempt fails and these containers will be retrieved by" + " the new application attempt ");
		opts.addOption("attempt_failures_validity_interval", true, "when attempt_failures_validity_interval in milliseconds is set to > 0," + "the failure number will not take failures which happen out of " + "the validityInterval into failure count. " + "If failure count reaches to maxAppAttempts, " + "the application will be failed.");
		opts.addOption("debug", false, "Dump out debug information");
		opts.addOption("help", false, "Print usage");
		opts.addOption("node_label_expression", true, "Node label expression to determine the nodes" + " where all the containers of this application" + " will be allocated, \"\" means containers" + " can be allocated anywhere, if you don't specify the option," + " default node_label_expression of queue will be used.");
		return opts;
	}

	/**
	 * Helper function to print out usage
	 */
	private static void printUsage() {
		new HelpFormatter().printHelp("Client", opts);
	}

	private static void dumpOutDebugInfo() {
		LOGGER.info("Dump debug output");
		Map<String, String> envs = System.getenv();
		for (Map.Entry<String, String> env : envs.entrySet()) {
			LOGGER.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
		}

		BufferedReader buf = null;
		try {
			String lines = Shell.WINDOWS ? Shell.execCommand("cmd", "/c", "dir") : Shell.execCommand("ls", "-al");
			buf = new BufferedReader(new StringReader(lines));
			String line = "";
			while ((line = buf.readLine()) != null) {
				LOGGER.info("System CWD content: " + line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.cleanup(LOGGER, buf);
		}
	}
}
