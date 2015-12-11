package com.infogen.yarn.application_master;

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
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.infogen.yarn.Constants;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月3日 下午4:56:10
 * @since 1.0
 * @version 1.0
 */
public class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
	private static final Log LOGGER = LogFactory.getLog(RMCallbackHandler.class);

	private final ApplicationMaster AM;
	private final ApplicationMaster_Configuration applicationmaster_configuration;
	private final NMCallbackHandler nmcallbackhandler;

	public RMCallbackHandler(ApplicationMaster applicationMaster, ApplicationMaster_Configuration applicationmaster_configuration, NMCallbackHandler nmcallbackhandler) {
		this.AM = applicationMaster;
		this.applicationmaster_configuration = applicationmaster_configuration;
		this.nmcallbackhandler = nmcallbackhandler;
	}

	@Override
	public void onContainersCompleted(List<ContainerStatus> completedContainers) {
		LOGGER.info("#onContainersCompleted, completedCnt=" + completedContainers.size());
		for (ContainerStatus containerStatus : completedContainers) {
			LOGGER.info("#container状态 appAttemptID =" + AM.appAttemptID + "  containerID=" + containerStatus.getContainerId() + ", state=" + containerStatus.getState() + ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics=" + containerStatus.getDiagnostics());
			assert (containerStatus.getState() == ContainerState.COMPLETE);
			int exitStatus = containerStatus.getExitStatus();
			// 失败的container
			if (exitStatus != 0) {
				if (ContainerExitStatus.ABORTED == exitStatus) {
					// 多种原因被框架kill
					AM.numAllocatedContainers.decrementAndGet();
					AM.numRequestedContainers.decrementAndGet();
				} else {
					// 执行失败
					AM.numCompletedContainers.incrementAndGet();
					AM.numFailedContainers.incrementAndGet();
				}
			} else {
				// container completed successfully
				AM.numCompletedContainers.incrementAndGet();
				LOGGER.info("#Container completed successfully." + ", containerId=" + containerStatus.getContainerId());
			}
		}

		// ask for more containers if any failed
		LOGGER.info("#numTotalContainers: " + applicationmaster_configuration.numTotalContainers + ", numRequestedContainers: " + AM.numRequestedContainers.get());
		int askCount = applicationmaster_configuration.numTotalContainers - AM.numRequestedContainers.get();
		AM.numRequestedContainers.addAndGet(askCount);

		LOGGER.info("#请求container数量: " + askCount);
		if (askCount > 0) {
			for (int i = 0; i < askCount; ++i) {
				ContainerRequest containerAsk = AM.setupContainerAskForRM();
				AM.amRMClient.addContainerRequest(containerAsk);
				LOGGER.info("Sent containerAsk 1");
			}
		}

	}

	@Override
	public void onContainersAllocated(List<Container> allocatedContainers) {
		LOGGER.info("#onContainersAllocated, completedCnt=" + allocatedContainers.size());
		AM.numAllocatedContainers.addAndGet(allocatedContainers.size());
		for (Container allocatedContainer : allocatedContainers) {
			LOGGER.info("#在new container中启动任务:" + "containerId=" + allocatedContainer.getId() + ", containerNode=" + allocatedContainer.getNodeId().getHost() + ":" + allocatedContainer.getNodeId().getPort() + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress() + ", containerResourceMemory" + allocatedContainer.getResource().getMemory() + ", containerResourceVirtualCores" + allocatedContainer.getResource().getVirtualCores());
			LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer, nmcallbackhandler);
			Thread launchThread = new Thread(runnableLaunchContainer);
			AM.launchThreads.add(launchThread);
			launchThread.start();
		}
	}

	@Override
	public void onShutdownRequest() {
		LOGGER.info("=====onShutdownRequest()=====");
	}

	@Override
	public void onNodesUpdated(List<NodeReport> updatedNodes) {
		LOGGER.info("=====onNodesUpdated()=====");
	}

	@Override
	public float getProgress() {
		// set progress to deliver to RM on next heartbeat
		float progress = (float) AM.numCompletedContainers.get() / applicationmaster_configuration.numTotalContainers;
		return progress;
	}

	@Override
	public void onError(Throwable e) {
		LOGGER.info("=====onError()=====, {}", e);
		AM.amRMClient.stop();
	}

	/**
	 * Thread to connect to the {@link ContainerManagementProtocol} and launch the container that will execute the command.
	 */
	private class LaunchContainerRunnable implements Runnable {
		protected Configuration conf = new YarnConfiguration();
		// Allocated container
		private Container container;
		private NMCallbackHandler containerListener;

		public LaunchContainerRunnable(Container lcontainer, NMCallbackHandler containerListener) {
			this.container = lcontainer;
			this.containerListener = containerListener;
		}

		@Override
		public void run() {
			LOGGER.info("提交任务: containerid=" + container.getId());
			// Set the environment
			Map<String, String> environment = new HashMap<String, String>();
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
			environment.put("USER", applicationmaster_configuration.user);

			// Set the local resources
			Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
			try {
				FileSystem fs = FileSystem.newInstance(conf);
				ApplicationId appId = this.container.getId().getApplicationAttemptId().getApplicationId();
				Path dst = new Path(fs.makeQualified(new Path("/user/" + applicationmaster_configuration.user)), Constants.APP_NAME + "/" + appId + "/" + Constants.JAR_NAME);
				LOGGER.info("#ocalResources:" + dst);
				FileStatus scFileStatus = fs.getFileStatus(dst);
				LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri()), LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, scFileStatus.getLen(), scFileStatus.getModificationTime());
				localResources.put(Constants.JAR_NAME, scRsrc);
			} catch (Exception e) {
				LOGGER.error("#ocalResources=", e);
			}

			// Set the necessary command to execute on the allocated container
			Vector<CharSequence> vargs = new Vector<CharSequence>(5);
			vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
			vargs.add("-Xmx" + applicationmaster_configuration.containerMemory + "m");
			vargs.add(Constants.JAVA_APPLICATION);
			vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
			vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
			// Get final commmand
			StringBuilder command = new StringBuilder();
			for (CharSequence str : vargs) {
				command.append(str).append(" ");
			}
			LOGGER.info("#command=" + command);
			List<String> commands = new ArrayList<String>();
			commands.add(command.toString());

			LOGGER.info("#startContainerAsync");
			ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(localResources, environment, commands, null, null, null);
			containerListener.addContainer(container.getId(), container);
			AM.nmClientAsync.startContainerAsync(container, ctx);
		}
	}
}
