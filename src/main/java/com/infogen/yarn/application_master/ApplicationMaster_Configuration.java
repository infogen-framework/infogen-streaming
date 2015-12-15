package com.infogen.yarn.application_master;

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

import com.infogen.mapper.InfoGen_Mapper;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月3日 下午3:58:44
 * @since 1.0
 * @version 1.0
 */
public class ApplicationMaster_Configuration {
	private static final Log LOGGER = LogFactory.getLog(ApplicationMaster.class);
	public String user = System.getProperty("user.name");
	public Integer numTotalContainers = 1;
	// Memory to request for the container on which the command will run
	public Integer containerMemory = 512;
	// VirtualCores to request for the container on which the command will run
	public Integer containerVirtualCores = 1;
	// Priority of the request
	public Integer requestPriority;
	public String app_attempt_id;

	/////////////////////////////////////////////////////////////////////////////////////
	public String app_name = "infogen-etl-kafka";
	public Class<? extends InfoGen_Mapper> mapper_clazz;
	public String topic;
	public String zookeeper;

	private Options opts = builder_applicationmaster();

	@SuppressWarnings("unchecked")
	public ApplicationMaster_Configuration(String[] args) throws ParseException, ClassNotFoundException {
		LOGGER.info("args=");
		for (String string : args) {
			LOGGER.info(string);
		}

		CommandLine cliParser = new GnuParser().parse(opts, args);

		if (cliParser.hasOption("help")) {
			printUsage(opts);
			System.exit(0);
		}

		topic = cliParser.getOptionValue("topic");
		zookeeper = cliParser.getOptionValue("zookeeper");
		app_name = cliParser.getOptionValue("app_name");
		if (topic == null || zookeeper == null || app_name == null) {
			printUsage(opts);
			System.exit(0);
		}
		mapper_clazz = (Class<? extends InfoGen_Mapper>) Class.forName(cliParser.getOptionValue("mapper_clazz"));

		if (cliParser.hasOption("debug")) {
			dumpOutDebugInfo();
		}
		user = cliParser.getOptionValue("user", System.getProperty("user.name"));
		containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "512"));
		containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
		numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
		if (numTotalContainers == 0) {
			throw new IllegalArgumentException("#containers 不能小于1");
		}
		requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
		app_attempt_id = cliParser.getOptionValue("app_attempt_id", "");
	}

	private void dumpOutDebugInfo() {
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

	public Options builder_applicationmaster() {
		Options opts = new Options();
		opts.addOption("user", true, "启动用户");
		opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used unless for testing purposes");
		opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the command");
		opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run the command");
		opts.addOption("num_containers", true, "No. of containers on which the command needs to be executed");
		opts.addOption("priority", true, "Application Priority. Default 0");
		opts.addOption("debug", false, "Dump out debug information");
		opts.addOption("help", false, "Print usage");
		opts.addOption("mapper_clazz", true, "mapper_clazz");
		opts.addOption("topic", true, "topic");
		opts.addOption("zookeeper", true, "zookeeper");
		return opts;
	}

	/**
	 * Helper function to print usage
	 *
	 * @param opts
	 *            Parsed command line options
	 */
	private void printUsage(Options opts) {
		new HelpFormatter().printHelp("ApplicationMaster", opts);
	}
}
