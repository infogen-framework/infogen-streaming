package com.infogen.yarn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

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
import org.apache.hadoop.util.ExitUtil;
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
import org.apache.log4j.LogManager;

import com.infogen.util.DefaultEntry;

/**
 * YarnClient提交任务到ApplicationMaster，并监控运行进度
 * @author larry/larrylv@outlook.com/创建时间 2015年12月21日 下午6:33:26
 * @since 1.0
 * @version 1.0
 */
public class InfoGen_Job {
	private static final Log LOGGER = LogFactory.getLog(InfoGen_Job.class);
	private Job_Configuration job_configuration;
	private String app_name;

	private Configuration conf = new YarnConfiguration();

	public InfoGen_Job(Job_Configuration job_configuration, String app_name) {
		this.job_configuration = job_configuration;
		this.app_name = app_name == null ? "infogen" : app_name;

	}

	public void submit() {
		if (job_configuration.zookeeper == null || job_configuration.topic == null || job_configuration.group == null) {
			LOGGER.error("zookeeper,topic,group不能为空");
			return;
		}
		if (job_configuration.parameters == null) {
			job_configuration.parameters = "";
		}

		DefaultEntry<ApplicationId, YarnClient> entry = null;
		try {
			entry = run();
		} catch (Throwable t) {
			LOGGER.fatal("#Error running Client", t);
			LogManager.shutdown();
			ExitUtil.terminate(1, t);
		}
		monitoring(entry.getKey(), entry.getValue());
	}

	public DefaultEntry<ApplicationId, YarnClient> run() throws IOException, YarnException {
		LOGGER.info("#启动 Client 可以通过 yarn application -kill application_xxx结束任务");
		YarnClient yarnClient = YarnClient.createYarnClient();
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
		QueueInfo queueInfo = yarnClient.getQueueInfo(job_configuration.amQueue);
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
		appContext.setApplicationName(app_name);
		// application出错的时候containers重用还是kill
		appContext.setKeepContainersAcrossApplicationAttempts(job_configuration.keepContainers);
		// 请求资源时指定的节点表达式 Now only support AND(&&), in the future will provide support for OR(||), NOT(!)
		if (null != job_configuration.nodeLabelExpression) {
			appContext.setNodeLabelExpression(job_configuration.nodeLabelExpression);
		}
		// 重试时间间隔
		if (job_configuration.attemptFailuresValidityInterval >= 0) {
			appContext.setAttemptFailuresValidityInterval(job_configuration.attemptFailuresValidityInterval);
		}
		// applicationManager启动资源/优先级/队列
		GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
		int maxMem = appResponse.getMaximumResourceCapability().getMemory();
		if (job_configuration.amMemory > maxMem) {
			LOGGER.info("#AM memory 配置过多:" + job_configuration.amMemory + ", 使用最大值:" + maxMem);
			job_configuration.amMemory = maxMem;
		}
		int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
		if (job_configuration.amVCores > maxVCores) {
			LOGGER.info("#AM virtual cores 配置过多:" + job_configuration.amVCores + ", 使用最大值:" + maxVCores);
			job_configuration.amVCores = maxVCores;
		}
		appContext.setResource(Resource.newInstance(job_configuration.amMemory, job_configuration.amVCores));
		appContext.setPriority(Priority.newInstance(job_configuration.amPriority));
		appContext.setQueue(job_configuration.amQueue);

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
		appContext.setAMContainerSpec(amContainer);
		LOGGER.info("#Submitting application");
		yarnClient.submitApplication(appContext);

		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		return new DefaultEntry<>(appContext.getApplicationId(), yarnClient);
	}

	private Map<String, LocalResource> copyFromLocalFile(ApplicationId applicationId, String scpath, String dstpath, Map<String, LocalResource> localResources) throws IOException {
		LOGGER.info("#copyFromLocalFile:" + Constants.LOCAL_JAR_PATH + " to " + Constants.JAR_NAME);
		FileSystem fs = FileSystem.get(conf);
		Path jar_dst = new Path(fs.makeQualified(new Path("/user/" + job_configuration.user)), app_name + "/" + applicationId + "/" + dstpath);
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
		environment.put("USER", job_configuration.user);
		return environment;
	}

	@SuppressWarnings("restriction")
	private List<String> commands() {
		// Set the necessary command to execute the application master
		Vector<CharSequence> vargs = new Vector<CharSequence>(30);
		vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
		vargs.add("-Xmx" + job_configuration.amMemory + "m");
		vargs.add(Constants.APPLICATIONMASTER_CLASS);
		vargs.add("--container_memory " + String.valueOf(job_configuration.containerMemory));
		vargs.add("--container_vcores " + String.valueOf(job_configuration.containerVirtualCores));
		vargs.add("--num_containers " + String.valueOf(job_configuration.numContainers));
		vargs.add("--user " + String.valueOf(job_configuration.user));

		vargs.add("--app_name " + String.valueOf(app_name));
		vargs.add("--zookeeper " + String.valueOf(job_configuration.zookeeper));
		vargs.add("--topic " + String.valueOf(job_configuration.topic));
		vargs.add("--group " + String.valueOf(job_configuration.group));
		vargs.add("--mapper_clazz " + String.valueOf(job_configuration.mapper_clazz.getName()));
		vargs.add("--parameters " + new sun.misc.BASE64Encoder().encode(job_configuration.parameters.getBytes()));
		if (job_configuration.debugFlag) {
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

	private void monitoring(ApplicationId appId, YarnClient yarnClient) {
		while (true) {
			try {
				Thread.sleep(10 * 1000);
			} catch (InterruptedException e) {
				LOGGER.debug("#Thread sleep in monitoring loop interrupted", e);
			}

			// Get application report for the appId we are interested in
			ApplicationReport report = null;
			try {
				report = yarnClient.getApplicationReport(appId);
			} catch (YarnException | IOException e) {
				LOGGER.fatal("#获取 application 状态失败", e);
				continue;
			}

			LOGGER.info("#获取 application 状态:" + ", appId=" + appId.getId() + ", clientToAMToken=" + report.getClientToAMToken() + ", appDiagnostics=" + report.getDiagnostics() + ", appMasterHost=" + report.getHost() + ", appQueue=" + report.getQueue());
			LOGGER.info("                                           , appMasterRpcPort=" + report.getRpcPort() + ", appStartTime=" + report.getStartTime() + ", yarnAppState=" + report.getYarnApplicationState().toString() + ", distributedFinalState=" + report.getFinalApplicationStatus().toString());
			LOGGER.info("                                          , appTrackingUrl=" + report.getTrackingUrl() + ", appUser=" + report.getUser());

			YarnApplicationState state = report.getYarnApplicationState();
			FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();

			if (YarnApplicationState.RUNNING == state) {
				LOGGER.info("#Application is running...");
				continue;
			} else if (YarnApplicationState.FINISHED == state) {
				if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
					LOGGER.info("#Application 执行成功");
				} else {
					LOGGER.info("#Application 执行完成，但有失败:" + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString() + ". Breaking monitoring loop");
				}
				return;
			} else if (YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state) {
				LOGGER.info("#Application 没有完成:" + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString() + ". Breaking monitoring loop");
				LogManager.shutdown();
				return;
			} else {
				LOGGER.fatal("#未知 application 状态");
				LogManager.shutdown();
				return;
			}
		}
	}
}