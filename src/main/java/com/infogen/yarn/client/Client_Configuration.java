package com.infogen.yarn.client;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月3日 上午11:08:05
 * @since 1.0
 * @version 1.0
 */
public class Client_Configuration {
	private static final Log LOGGER = LogFactory.getLog(Client.class);
	public String LOCAL_LOG4J_PATH = "";
	// 优先级
	public Integer amPriority = 0;
	// 执行队列
	public String amQueue = "";
	// 内存 for to run the App Master
	public Integer amMemory = 10;
	// cpu 核数 for to run the App Master
	public Integer amVCores = 1;
	// Amt of memory to request for container in which command will be executed
	public Integer containerMemory = 10;
	// Amt. of virtual cores to request for container in which command will be executed
	public Integer containerVirtualCores = 1;
	// No. of containers in which the command needs to be executed
	public Integer numContainers = 1;

	public String nodeLabelExpression = null;

	// Start time for client
	public final long clientStartTime = System.currentTimeMillis();
	// Timeout threshold for client. Kill app after time Integererval expires.
	public long clientTimeout = 60 * 1000;
	// flag to indicate whether to keep containers across application attempts.
	public Boolean keepContainers = false;
	public long attemptFailuresValidityInterval = -1;
	// Debug flag
	public Boolean debugFlag = false;

	// Command line options
	private Options opts = builder_client();

	public Client_Configuration(String[] args) throws ParseException {
		LOGGER.info("Initializing Client");
		if (args.length == 0) {
			LOGGER.error("No args specified for client to initialize");
			printUsage();
			System.exit(-1);
		}

		CommandLine cliParser = new GnuParser().parse(opts, args);
		if (cliParser.hasOption("help")) {
			printUsage();
			System.exit(0);
		}
		if (cliParser.hasOption("debug")) {
			LOGGER.info("debug");
			debugFlag = true;
		}

		LOCAL_LOG4J_PATH = cliParser.getOptionValue("log_properties", "");
		amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
		amQueue = cliParser.getOptionValue("queue", "default");
		nodeLabelExpression = cliParser.getOptionValue("node_label_expression", null);
		clientTimeout = Integer.parseInt(cliParser.getOptionValue("timeout", "60000"));
		if (cliParser.hasOption("keep_containers_across_application_attempts")) {
			LOGGER.info("keep_containers_across_application_attempts");
			keepContainers = true;
		}
		attemptFailuresValidityInterval = Long.parseLong(cliParser.getOptionValue("attempt_failures_validity_interval", "-1"));

		amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "10"));
		amVCores = Integer.parseInt(cliParser.getOptionValue("master_vcores", "1"));
		containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
		containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
		numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
		if (amMemory < 0) {
			throw new IllegalArgumentException("Invalid memory specified for application master, exiting." + " Specified memory=" + amMemory);
		}
		if (amVCores < 0) {
			throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting." + " Specified virtual cores=" + amVCores);
		}
		if (containerMemory < 0) {
			throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified," + " exiting." + " Specified containerMemory=" + containerMemory);
		}
		if (containerVirtualCores < 0) {
			throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified," + " exiting." + " Specified  containerVirtualCores=" + containerVirtualCores);
		}
		if (numContainers < 1) {
			throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified," + " exiting." + " Specified numContainer=" + numContainers);
		}

	}

	public Options builder_client() {
		Options opts = new Options();
		opts.addOption("priority", true, "Application Priority. Default 0");
		opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
		opts.addOption("timeout", true, "Application timeout in milliseconds");
		opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
		opts.addOption("master_vcores", true, "Amount of virtual cores to be requested to run the application master");
		opts.addOption("jar", true, "Jar file containing the application master");
		opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the command");
		opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run the command");
		opts.addOption("num_containers", true, "No. of containers on which the command needs to be executed");
		opts.addOption("log_properties", true, "log4j.properties file");
		opts.addOption("keep_containers_across_application_attempts", false, "Flag to indicate whether to keep containers across application attempts." + " If the flag is true, running containers will not be killed when" + " application attempt fails and these containers will be retrieved by" + " the new application attempt ");
		opts.addOption("attempt_failures_validity_interval", true, "when attempt_failures_validity_interval in milliseconds is set to > 0," + "the failure number will not take failures which happen out of " + "the validityInterval into failure count. " + "If failure count reaches to maxAppAttempts, " + "the application will be failed.");
		opts.addOption("debug", false, "Dump out debug information");
		opts.addOption("help", false, "Print usage");
		opts.addOption("node_label_expression", true, "Node label expression to determine the nodes" + " where all the containers of this application" + " will be allocated, \"\" means containers" + " can be allocated anywhere, if you don't specify the option," + " default node_label_expression of queue will be used.");
		return opts;
	}

	/**
	 * Helper function to print out usage
	 */
	private void printUsage() {
		new HelpFormatter().printHelp("Client", opts);
	}
}
