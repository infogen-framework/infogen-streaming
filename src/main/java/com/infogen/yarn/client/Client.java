
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
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
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
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.infogen.yarn.Job_Configuration;
import com.infogen.yarn.Constants;

public class Client {
	private static final Log LOGGER = LogFactory.getLog(Client.class);
	public YarnClient yarnClient;
	private Job_Configuration client_configuration;
	private Configuration conf = new YarnConfiguration();

	public Client(Job_Configuration client_configuration) throws ParseException {
		this.client_configuration = client_configuration;
	}

	public ApplicationId run() throws IOException, YarnException {
		LOGGER.info("#启动 Client");
		yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		yarnClient.start();

		///////////////////////////////////////////////////////// 打印集群状态
		// LOGGER 集群指标 目前只提供NodeManager的个数
		YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
		LOGGER.info("#NodeManager 个数:" + clusterMetrics.getNumNodeManagers());
		List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
		LOGGER.info("#Node状态:");
		for (NodeReport node : clusterNodeReports) {
			LOGGER.info("#nodeId=" + node.getNodeId() + ", nodeAddress" + node.getHttpAddress() + ", nodeRackName" + node.getRackName() + ", nodeNumContainers" + node.getNumContainers());
		}
		QueueInfo queueInfo = yarnClient.getQueueInfo(client_configuration.amQueue);
		LOGGER.info("#队列信息:  queueName=" + queueInfo.getQueueName() + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity() + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity() + ", queueApplicationCount=" + queueInfo.getApplications().size() + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());
		List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
		for (QueueUserACLInfo aclInfo : listAclInfo) {
			for (QueueACL userAcl : aclInfo.getUserAcls()) {
				LOGGER.info("#User ACL Info for Queue" + ", queueName=" + aclInfo.getQueueName() + ", userAcl=" + userAcl.name());
			}
		}

		//////////////////////////////////////////////// 配置appContext
		YarnClientApplication app = yarnClient.createApplication();
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		appContext.setApplicationName(client_configuration.app_name);
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
			LOGGER.info("#AM memory 配置过多:" + client_configuration.amMemory + ", 使用最大值:" + maxMem);
			client_configuration.amMemory = maxMem;
		}
		int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
		if (client_configuration.amVCores > maxVCores) {
			LOGGER.info("#AM virtual cores 配置过多:" + client_configuration.amVCores + ", 使用最大值:" + maxVCores);
			client_configuration.amVCores = maxVCores;
		}
		appContext.setResource(Resource.newInstance(client_configuration.amMemory, client_configuration.amVCores));
		appContext.setPriority(Priority.newInstance(client_configuration.amPriority));
		appContext.setQueue(client_configuration.amQueue);

		//////////////////////////////////////////////// 获取amContainer
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		copyFromLocalFile(appContext.getApplicationId(), Constants.LOCAL_JAR_PATH, Constants.JAR_NAME, localResources);
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
			LOGGER.info("#SecurityEnabled , not support");
		}

		////////////////////////////////////////// 启动
		// Set the necessary security tokens as needed
		// amContainer.setContainerTokens(containerToken);
		appContext.setAMContainerSpec(amContainer);
		LOGGER.info("#Submitting application");
		yarnClient.submitApplication(appContext);

		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		// Monitor the application
		return appContext.getApplicationId();
	}

	private Map<String, LocalResource> copyFromLocalFile(ApplicationId applicationId, String scpath, String dstpath, Map<String, LocalResource> localResources) throws IOException {
		LOGGER.info("#copyFromLocalFile:" + Constants.LOCAL_JAR_PATH + " to " + Constants.JAR_NAME);
		FileSystem fs = FileSystem.get(conf);
		Path jar_dst = new Path(fs.makeQualified(new Path("/user/" + client_configuration.user)), client_configuration.app_name + "/" + applicationId + "/" + dstpath);
		LOGGER.info("#jar_dst:" + jar_dst);
		fs.copyFromLocalFile(new Path(scpath), jar_dst);
		FileStatus jar_scFileStatus = fs.getFileStatus(jar_dst);
		localResources.put(dstpath, LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(jar_dst.toUri()), LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, jar_scFileStatus.getLen(), jar_scFileStatus.getModificationTime()));
		return localResources;
	}

	private Map<String, String> environment() {
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
		LOGGER.info("#environment :" + classPathEnv.toString());
		environment.put("CLASSPATH", classPathEnv.toString());
		environment.put("USER", client_configuration.user);
		return environment;
	}

	private List<String> commands() {
		// Set the necessary command to execute the application master
		Vector<CharSequence> vargs = new Vector<CharSequence>(30);
		vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
		vargs.add("-Xmx" + client_configuration.amMemory + "m");
		vargs.add(Constants.APPLICATIONMASTER_CLASS);
		vargs.add("--container_memory " + String.valueOf(client_configuration.containerMemory));
		vargs.add("--container_vcores " + String.valueOf(client_configuration.containerVirtualCores));
		vargs.add("--num_containers " + String.valueOf(client_configuration.numContainers));
		vargs.add("--user " + String.valueOf(client_configuration.user));
		vargs.add("--mapper_clazz " + String.valueOf(client_configuration.mapper_clazz.getName()));
		vargs.add("--topic " + String.valueOf(client_configuration.topic));
		vargs.add("--zookeeper " + String.valueOf(client_configuration.zookeeper));
		vargs.add("--app_name " + String.valueOf(client_configuration.app_name));

		if (client_configuration.debugFlag) {
			vargs.add("--debug");
		}
		vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
		vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

		StringBuilder command = new StringBuilder();
		for (CharSequence str : vargs) {
			command.append(str).append(" ");
		}

		LOGGER.info("#command:" + command);
		List<String> commands = new ArrayList<String>();
		commands.add(command.toString());
		return commands;
	}

	/**
	 * @param args
	 *            Command line arguments
	 */
	public static void main(String[] args) {

	}
}