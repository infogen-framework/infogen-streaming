package com.infogen.yarn.application_master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
	private static final Log LOG = LogFactory.getLog(RMCallbackHandler.class);

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
		LOG.info("Got response from RM for container ask, completedCnt=" + completedContainers.size());
		for (ContainerStatus containerStatus : completedContainers) {
			LOG.info(AM.appAttemptID + " got container status for containerID=" + containerStatus.getContainerId() + ", state=" + containerStatus.getState() + ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics=" + containerStatus.getDiagnostics());
			// non complete containers should not be here
			assert (containerStatus.getState() == ContainerState.COMPLETE);
			// increment counters for completed/failed containers
			int exitStatus = containerStatus.getExitStatus();
			if (0 != exitStatus) {
				// container failed
				if (ContainerExitStatus.ABORTED == exitStatus) {
					// container was killed by framework, possibly preempted
					// we should re-try as the container was lost for some reason
					AM.numAllocatedContainers.decrementAndGet();
					AM.numRequestedContainers.decrementAndGet();
				} else {
					// command failed
					// counts as completed
					AM.numCompletedContainers.incrementAndGet();
					AM.numFailedContainers.incrementAndGet();
				}
			} else {
				// nothing to do
				// container completed successfully
				AM.numCompletedContainers.incrementAndGet();
				LOG.info("Container completed successfully." + ", containerId=" + containerStatus.getContainerId());
			}
		}

		// ask for more containers if any failed
		LOG.info("numTotalContainers: " + applicationmaster_configuration.numTotalContainers + ", numRequestedContainers: " + AM.numRequestedContainers.get());
		int askCount = applicationmaster_configuration.numTotalContainers - AM.numRequestedContainers.get();
		AM.numRequestedContainers.addAndGet(askCount);

		LOG.info("askCount: " + askCount);
		if (askCount > 0) {
			for (int i = 0; i < askCount; ++i) {
				ContainerRequest containerAsk = AM.setupContainerAskForRM();
				AM.amRMClient.addContainerRequest(containerAsk);
				LOG.info("Sent containerAsk 1");
			}
		}

	}

	@Override
	public void onContainersAllocated(List<Container> allocatedContainers) {
		LOG.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
		AM.numAllocatedContainers.addAndGet(allocatedContainers.size());
		for (Container allocatedContainer : allocatedContainers) {
			LOG.info("Launching command on a new container." + ", containerId=" + allocatedContainer.getId() + ", containerNode=" + allocatedContainer.getNodeId().getHost() + ":" + allocatedContainer.getNodeId().getPort() + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress() + ", containerResourceMemory" + allocatedContainer.getResource().getMemory() + ", containerResourceVirtualCores" + allocatedContainer.getResource().getVirtualCores());
			LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer, nmcallbackhandler);
			Thread launchThread = new Thread(runnableLaunchContainer);
			AM.launchThreads.add(launchThread);
			launchThread.start();
		}
	}

	@Override
	public void onShutdownRequest() {
		LOG.info("=====onShutdownRequest()=====");
	}

	@Override
	public void onNodesUpdated(List<NodeReport> updatedNodes) {
		LOG.info("=====onNodesUpdated()=====");
	}

	@Override
	public float getProgress() {
		// set progress to deliver to RM on next heartbeat
		float progress = (float) AM.numCompletedContainers.get() / applicationmaster_configuration.numTotalContainers;
		return progress;
	}

	@Override
	public void onError(Throwable e) {
		LOG.info("=====onError()=====, {}", e);
		AM.amRMClient.stop();
	}

	/**
	 * Thread to connect to the {@link ContainerManagementProtocol} and launch the container that will execute the command.
	 */
	private class LaunchContainerRunnable implements Runnable {
		// Allocated container
		private Container container;
		private NMCallbackHandler containerListener;

		public LaunchContainerRunnable(Container lcontainer, NMCallbackHandler containerListener) {
			this.container = lcontainer;
			this.containerListener = containerListener;
		}

		@Override
		/**
		 * Connects to CM, sets up container launch context for command and eventually dispatches the container start request to the CM.
		 */
		public void run() {
			LOG.info("Setting up container launch container for containerid=" + container.getId());
			// Set the environment
			Map<String, String> env = new HashMap<>();
			StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$()).append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
			for (String c : AM.conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
				classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
				classPathEnv.append(c.trim());
			}
			env.put("CLASSPATH", classPathEnv.toString());

			// Set the local resources
			Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
			try {
				FileSystem fs = FileSystem.newInstance(AM.conf);
				ApplicationId appId = this.container.getId().getApplicationAttemptId().getApplicationId();
				String suffix = Constants.APP_NAME + "/" + appId + "/" + Constants.JAR_NAME;
				Path dst = new Path(fs.getHomeDirectory(), suffix);
				FileStatus scFileStatus = fs.getFileStatus(dst);
				LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri()), LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, scFileStatus.getLen(), scFileStatus.getModificationTime());
				localResources.put(Constants.JAR_NAME, scRsrc);
			} catch (Exception e) {
				e.printStackTrace();
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

			List<String> commands = new ArrayList<String>();
			commands.add(command.toString());

			ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(localResources, env, commands, null, AM.allTokens.duplicate(), null);
			containerListener.addContainer(container.getId(), container);
			AM.nmClientAsync.startContainerAsync(container, ctx);
		}
	}
}
