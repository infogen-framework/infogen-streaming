package com.infogen.yarn.application_master;

import java.io.BufferedReader;
import java.io.File;
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

import com.infogen.yarn.Constants;
import com.infogen.yarn.Log4jPropertyHelper;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月3日 下午3:58:44
 * @since 1.0
 * @version 1.0
 */
public class ApplicationMaster_Configuration {
	private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
	public Integer numTotalContainers = 1;
	// Memory to request for the container on which the command will run
	public Integer containerMemory = 10;
	// VirtualCores to request for the container on which the command will run
	public Integer containerVirtualCores = 1;
	// Priority of the request
	public Integer requestPriority;
	public String app_attempt_id;

	private Options opts = builder_applicationmaster();

	public ApplicationMaster_Configuration(String[] args) throws ParseException {
		if (args.length == 0) {
			LOG.error("No args specified for client to initialize");
			printUsage(opts);
			System.exit(-1);
		}

		CommandLine cliParser = new GnuParser().parse(opts, args);

		if (cliParser.hasOption("help")) {
			printUsage(opts);
			System.exit(0);
		}

		// Check whether customer log4j.properties file exists
		if (new File(Constants.LOG4J_PATH).exists()) {
			try {
				Log4jPropertyHelper.updateLog4jConfiguration(ApplicationMaster.class, Constants.LOG4J_PATH);
			} catch (Exception e) {
				LOG.warn("Can not set up custom log4j properties. " + e);
			}
		}

		if (cliParser.hasOption("debug")) {
			dumpOutDebugInfo();
		}

		containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
		containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
		numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
		if (numTotalContainers == 0) {
			throw new IllegalArgumentException("Cannot run distributed shell with no containers");
		}
		requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
		app_attempt_id = cliParser.getOptionValue("app_attempt_id", "");
	}

	/**
	 * Dump out contents of $CWD and the environment to stdout for debugging
	 */
	private void dumpOutDebugInfo() {
		LOG.info("Dump debug output");
		Map<String, String> envs = System.getenv();
		for (Map.Entry<String, String> env : envs.entrySet()) {
			LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
			System.out.println("System env: key=" + env.getKey() + ", val=" + env.getValue());
		}

		BufferedReader buf = null;
		try {
			String lines = Shell.WINDOWS ? Shell.execCommand("cmd", "/c", "dir") : Shell.execCommand("ls", "-al");
			buf = new BufferedReader(new StringReader(lines));
			String line = "";
			while ((line = buf.readLine()) != null) {
				LOG.info("System CWD content: " + line);
				System.out.println("System CWD content: " + line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.cleanup(LOG, buf);
		}
	}

	public Options builder_applicationmaster() {
		Options opts = new Options();
		opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used unless for testing purposes");
		opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the command");
		opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run the command");
		opts.addOption("num_containers", true, "No. of containers on which the command needs to be executed");
		opts.addOption("priority", true, "Application Priority. Default 0");
		opts.addOption("debug", false, "Dump out debug information");
		opts.addOption("help", false, "Print usage");
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
