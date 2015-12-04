package com.infogen.yarn3;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Logger;

/**
 * 
 * @author larry/larrylv@outlook.com/创建时间 2015年12月1日 下午5:47:11
 * @since 1.0
 * @version 1.0
 */
public class InfoGen_Yarn_Client {
	private static Logger LOGGER = Logger.getLogger(InfoGen_Yarn_Client.class);

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (UserGroupInformation.isSecurityEnabled()) {
			throw new Exception("SecurityEnabled , not support");
		}

		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		yarnClient.start();

		YarnClientApplication app = yarnClient.createApplication();
		ApplicationSubmissionContext context = app.getApplicationSubmissionContext();

		context.setApplicationName("truman.ApplicationMaster");
		// ?
		context.setKeepContainersAcrossApplicationAttempts(false);
		// 设置Applicaton 的 内存 和 cpu 需求 以及 优先级和 Queue 信息， YARN将根据这些信息来调度App Master
		context.setResource(Resource.newInstance(100, 1));// int memory, int vCores
		context.setPriority(Priority.newInstance(0));
		context.setQueue("default");

		// amContainer中包含了App Master 执行需要的资源文件，环境变量 和 启动命令，这里将资源文件上传到了HDFS，这样在NODE Manager 就可以通过 HDFS取得这些文件
		ContainerLaunchContext amContainer = createAMContainerLanunchContext(conf, context.getApplicationId());
		context.setAMContainerSpec(amContainer);

		// 提交应用
		ApplicationId appId = yarnClient.submitApplication(context);

		monitorApplicationReport(yarnClient, appId);

	}

	private static ContainerLaunchContext createAMContainerLanunchContext(Configuration conf, ApplicationId appId) throws IOException {
		// 上传jar文件到hdfs
		String local_jar_path = ClassUtil.findContainingJar(InfoGen_Yarn_Client.class);
		String jar_name = FilenameUtils.getName(local_jar_path);

		FileSystem fs = FileSystem.get(conf);
		Path dst = new Path(fs.getHomeDirectory(), "infogen_etl" + "/" + appId + "/" + jar_name);
		LOGGER.info("hdfs copyFromLocalFile " + local_jar_path + " =>" + dst);
		fs.copyFromLocalFile(new Path(local_jar_path), dst);

		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		FileStatus scFileStatus = fs.getFileStatus(dst);
		LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromPath(dst), LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, scFileStatus.getLen(), scFileStatus.getModificationTime());
		localResources.put(jar_name, scRsrc);

		// 设置 CLASSPATH environment
		Map<String, String> environment = new HashMap<String, String>();
		StringBuilder classpath_environment = new StringBuilder(Environment.CLASSPATH.$$());
		classpath_environment.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
		classpath_environment.append("./*");
		for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
			classpath_environment.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			classpath_environment.append(c.trim());
		}
		if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
			classpath_environment.append(':');
			classpath_environment.append(System.getProperty("java.class.path"));
		}
		LOGGER.info("classpath_environment:" + classpath_environment.toString());
		environment.put(Environment.CLASSPATH.name(), classpath_environment.toString());

		// 生成执行命令 execute command
		List<String> commands = new LinkedList<String>();
		StringBuilder command = new StringBuilder();
		command.append(Environment.JAVA_HOME.$$()).append("/bin/java  ");
		command.append("-Dlog4j.configuration=container-log4j.properties ");
		command.append("-Dyarn.app.container.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + " ");
		command.append("-Dyarn.app.container.log.filesize=0 ");
		command.append("-Dhadoop.root.logger=INFO,CLA ");
		command.append("com.infogen.yarn.InfoGen_Yarn_ApplicationMaster ");
		command.append("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout ");
		command.append("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr ");
		commands.add(command.toString());

		ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(localResources, environment, commands, null, null, null);
		return amContainer;
	}

	private static void monitorApplicationReport(YarnClient yarnClient, ApplicationId appId) throws YarnException, IOException {
		while (true) {
			try {
				Thread.sleep(5 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			ApplicationReport report = yarnClient.getApplicationReport(appId);
			LOGGER.info("Got application report " + ", clientToAMToken=" + report.getClientToAMToken() + ", appDiagnostics=" + report.getDiagnostics() + ", appMasterHost=" + report.getHost() + ", appQueue=" + report.getQueue() + ", appMasterRpcPort=" + report.getRpcPort() + ", appStartTime=" + report.getStartTime() + ", yarnAppState=" + report.getYarnApplicationState().toString() + ", distributedFinalState=" + report.getFinalApplicationStatus().toString() + ", appTrackingUrl="
					+ report.getTrackingUrl() + ", appUser=" + report.getUser());
		}
	}
}
