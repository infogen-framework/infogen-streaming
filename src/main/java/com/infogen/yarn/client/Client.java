
package com.infogen.yarn.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.infogen.yarn.Constants;

public class Client {
	private static final Log LOGGER = LogFactory.getLog(Client.class);
	private YarnClient yarnClient;
	private Client_Configuration client_configuration;
	private Configuration conf = new YarnConfiguration();

	public Client(Client_Configuration client_configuration) throws ParseException {
		this.client_configuration = client_configuration;
	}

	public boolean run() throws IOException, YarnException {
		LOGGER.info("Running Client");
		yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		yarnClient.start();

		///////////////////////////////////////////////////////// 打印集群状态
		// LOGGER 集群指标 目前只提供NodeManager的个数
		YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
		LOGGER.info("NodeManager 个数:" + clusterMetrics.getNumNodeManagers());
		List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
		LOGGER.info("Node状态:");
		for (NodeReport node : clusterNodeReports) {
			LOGGER.info("nodeId=" + node.getNodeId() + ", nodeAddress" + node.getHttpAddress() + ", nodeRackName" + node.getRackName() + ", nodeNumContainers" + node.getNumContainers());
		}
		QueueInfo queueInfo = yarnClient.getQueueInfo(client_configuration.amQueue);
		LOGGER.info("队列信息:  queueName=" + queueInfo.getQueueName() + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity() + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity() + ", queueApplicationCount=" + queueInfo.getApplications().size() + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());
		List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
		for (QueueUserACLInfo aclInfo : listAclInfo) {
			for (QueueACL userAcl : aclInfo.getUserAcls()) {
				LOGGER.info("User ACL Info for Queue" + ", queueName=" + aclInfo.getQueueName() + ", userAcl=" + userAcl.name());
			}
		}

		//////////////////////////////////////////////// 配置appContext
		YarnClientApplication app = yarnClient.createApplication();
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		appContext.setApplicationName(Constants.APP_NAME);
		// application出错的时候containers重用还是kill
		appContext.setKeepContainersAcrossApplicationAttempts(client_configuration.keepContainers);
		// 请求资源时指定的节点表达式 Now only support AND(&&), in the future will provide support for OR(||), NOT(!)
		if (null != client_configuration.nodeLabelExpression) {
			appContext.setNodeLabelExpression(client_configuration.nodeLabelExpression);
		}
		// 重试时间间隔
		if (client_configuration.attemptFailuresValidityInterval >= 0) {
			appContext.setAttemptFailuresValidityInterval(client_configuration.attemptFailuresValidityInterval);
		}
		// applicationManager启动资源/优先级/队列
		GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
		int maxMem = appResponse.getMaximumResourceCapability().getMemory();
		if (client_configuration.amMemory > maxMem) {
			LOGGER.info("AM memory 配置过多:" + client_configuration.amMemory + ", 使用最大值:" + maxMem);
			client_configuration.amMemory = maxMem;
		}
		int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
		if (client_configuration.amVCores > maxVCores) {
			LOGGER.info("AM virtual cores 配置过多:" + client_configuration.amVCores + ", 使用最大值:" + maxVCores);
			client_configuration.amVCores = maxVCores;
		}
		appContext.setResource(Resource.newInstance(client_configuration.amMemory, client_configuration.amVCores));
		appContext.setPriority(Priority.newInstance(client_configuration.amPriority));
		appContext.setQueue(client_configuration.amQueue);

		//////////////////////////////////////////////// 获取amContainer
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		copyFromLocalFile(appContext.getApplicationId(), Constants.LOCAL_JAR_PATH, Constants.JAR_NAME, localResources);
		if (!client_configuration.LOCAL_LOG4J_PATH.isEmpty()) {
			copyFromLocalFile(appContext.getApplicationId(), client_configuration.LOCAL_LOG4J_PATH, Constants.LOG4J_PATH, localResources);
		}
		Map<String, String> environment = environment();
		List<String> commands = commands();

		ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(localResources, environment, commands, null, null, null);
		// Setup security tokens
		if (UserGroupInformation.isSecurityEnabled()) {
			FileSystem fs = FileSystem.get(conf);
			// Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
			Credentials credentials = new Credentials();
			String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
			if (tokenRenewer == null || tokenRenewer.length() == 0) {
				throw new IOException("Can't get Master Kerberos principal for the RM to use as renewer");
			}
			// For now, only getting tokens for the default file-system.
			final Token<?> tokens[] = fs.addDelegationTokens(tokenRenewer, credentials);
			if (tokens != null) {
				for (Token<?> token : tokens) {
					LOGGER.info("Got dt for " + fs.getUri() + "; " + token);
				}
			}
			DataOutputBuffer dob = new DataOutputBuffer();
			credentials.writeTokenStorageToStream(dob);
			ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
			amContainer.setTokens(fsTokens);
		} else {
			LOGGER.info("SecurityEnabled , not support");
		}

		////////////////////////////////////////// 启动
		// Set the necessary security tokens as needed
		// amContainer.setContainerTokens(containerToken);
		appContext.setAMContainerSpec(amContainer);
		LOGGER.info("Submitting application to ASM");
		yarnClient.submitApplication(appContext);

		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		// Monitor the application
		return monitorApplication(appContext.getApplicationId());
	}

	private Map<String, LocalResource> copyFromLocalFile(ApplicationId applicationId, String scpath, String dstpath, Map<String, LocalResource> localResources) throws IOException {
		LOGGER.info("Copy App Master jar from local filesystem and add to local environment");
		FileSystem fs = FileSystem.get(conf);
		Path jar_dst = new Path(fs.getHomeDirectory(), Constants.APP_NAME + "/" + applicationId + "/" + dstpath);
		fs.copyFromLocalFile(new Path(scpath), jar_dst);
		FileStatus jar_scFileStatus = fs.getFileStatus(jar_dst);
		localResources.put(dstpath, LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(jar_dst.toUri()), LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, jar_scFileStatus.getLen(), jar_scFileStatus.getModificationTime()));
		return localResources;
	}

	private Map<String, String> environment() {
		LOGGER.info("Set the environment for the application master");
		Map<String, String> environment = new HashMap<String, String>();
		// Add AppMaster.jar location to classpath
		// At some point we should not be required to add
		// the hadoop specific classpaths to the env.
		// It should be provided out of the box.
		// For now setting all required classpaths including
		// the classpath to "." for the application jar
		StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$()).append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
		for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
			classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			classPathEnv.append(c.trim());
		}
		classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");
		// add the runtime classpath needed for tests to work
		if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
			classPathEnv.append(':');
			classPathEnv.append(System.getProperty("java.class.path"));
		}
		LOGGER.info("environment :" + classPathEnv.toString());
		environment.put("CLASSPATH", classPathEnv.toString());
		return environment;
	}

	private List<String> commands() {
		LOGGER.info("Setting up app master command");
		// Set the necessary command to execute the application master
		Vector<CharSequence> vargs = new Vector<CharSequence>(30);
		vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
		vargs.add("-Xmx" + client_configuration.amMemory + "m");
		vargs.add(Constants.APPLICATIONMASTER_CLASS);
		vargs.add("--container_memory " + String.valueOf(client_configuration.containerMemory));
		vargs.add("--container_vcores " + String.valueOf(client_configuration.containerVirtualCores));
		vargs.add("--num_containers " + String.valueOf(client_configuration.numContainers));
		if (client_configuration.debugFlag) {
			vargs.add("--debug");
		}
		vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
		vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

		StringBuilder command = new StringBuilder();
		for (CharSequence str : vargs) {
			command.append(str).append(" ");
		}

		List<String> commands = new ArrayList<String>();
		commands.add(command.toString());
		return commands;
	}

	/**
	 * Monitor the submitted application for completion. Kill application if time expires.
	 * 
	 * @param appId
	 *            Application Id of application to be monitored
	 * @return true if application completed successfully
	 * @throws YarnException
	 * @throws IOException
	 */
	private boolean monitorApplication(ApplicationId appId) throws YarnException, IOException {
		while (true) {
			// Check app status every 1 second.
			try {
				Thread.sleep(3 * 1000);
			} catch (InterruptedException e) {
				LOGGER.debug("Thread sleep in monitoring loop interrupted");
			}

			// Get application report for the appId we are interested in
			ApplicationReport report = yarnClient.getApplicationReport(appId);
			LOGGER.info("Got application report from ASM for" + ", appId=" + appId.getId() + ", clientToAMToken=" + report.getClientToAMToken() + ", appDiagnostics=" + report.getDiagnostics() + ", appMasterHost=" + report.getHost() + ", appQueue=" + report.getQueue() + ", appMasterRpcPort=" + report.getRpcPort() + ", appStartTime=" + report.getStartTime() + ", yarnAppState=" + report.getYarnApplicationState().toString() + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
					+ ", appTrackingUrl=" + report.getTrackingUrl() + ", appUser=" + report.getUser());

			YarnApplicationState state = report.getYarnApplicationState();
			FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
			if (YarnApplicationState.FINISHED == state) {
				if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
					LOGGER.info("Application has completed successfully. Breaking monitoring loop");
					return true;
				} else {
					LOGGER.info("Application did finished unsuccessfully." + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString() + ". Breaking monitoring loop");
					return false;
				}
			}
			if (YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state) {
				LOGGER.info("Application did not finish." + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString() + ". Breaking monitoring loop");
				return false;
			}
			// Running
			if (YarnApplicationState.RUNNING == state) {
				LOGGER.info("Application is running. Breaking monitoring loop.");
				return true;
			}
			// Client timeout
			if (System.currentTimeMillis() > (client_configuration.clientStartTime + client_configuration.clientTimeout)) {
				LOGGER.info("Reached client specified timeout for application. Killing application");
				yarnClient.killApplication(appId);
				return false;
			}
		}
	}

	/**
	 * @param args
	 *            Command line arguments
	 */
	public static void main(String[] args) {
		// hadoop jar yarn-app-example-0.0.1-SNAPSHOT.jar timo.yarn_app_call_java_daemon.Client
		// -jar yarn-app-example-0.0.1-SNAPSHOT.jar
		// -num_containers 2
		try {
			Client client = new Client(new Client_Configuration(args));
			if (client.run()) {
				LOGGER.info("Application completed successfully");
				System.exit(0);
			} else {
				LOGGER.error("Application failed to complete successfully");
				System.exit(2);
			}
		} catch (Throwable t) {
			LOGGER.fatal("Error running Client", t);
			System.exit(1);
		}
	}
}