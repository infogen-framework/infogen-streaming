package com.infogen.yarn.application_master;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;

public class ApplicationMaster {
	private static final Log LOGGER = LogFactory.getLog(ApplicationMaster.class);

	public static enum DSEvent {
		DS_APP_ATTEMPT_START, DS_APP_ATTEMPT_END, DS_CONTAINER_START, DS_CONTAINER_END
	}

	public static enum DSEntity {
		DS_APP_ATTEMPT, DS_CONTAINER
	}

	protected ApplicationMaster_Configuration applicationmaster_configuration;
	// Configuration
	protected Configuration conf = new YarnConfiguration();
	// Handle to communicate with the Resource Manager
	protected AMRMClientAsync<ContainerRequest> amRMClient;
	// Handle to communicate with the Node Manager
	protected NMClientAsync nmClientAsync;

	// 发送请求的数量/准备好的数量/完成的数量/失败的数量
	// Needed as once requested, we should not request for containers again.
	protected AtomicInteger numRequestedContainers = new AtomicInteger();
	protected AtomicInteger numAllocatedContainers = new AtomicInteger();
	protected AtomicInteger numCompletedContainers = new AtomicInteger();
	protected AtomicInteger numFailedContainers = new AtomicInteger();

	// Launch threads
	protected List<Thread> launchThreads = new ArrayList<Thread>();

	// Application Attempt Id ( combination of attemptId and fail count )
	protected ApplicationAttemptId appAttemptID;

	// In both secure and non-secure modes, this points to the job-submitter.

	public ApplicationMaster(ApplicationMaster_Configuration applicationmaster_configuration) throws ParseException {
		this.applicationmaster_configuration = applicationmaster_configuration;
	}

	public void check_environment() {
		Map<String, String> envs = System.getenv();
		if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
			if (applicationmaster_configuration.app_attempt_id.isEmpty()) {
				throw new IllegalArgumentException("Application Attempt Id not set in the environment");
			} else {
				appAttemptID = ConverterUtils.toApplicationAttemptId(applicationmaster_configuration.app_attempt_id);
			}
		} else {
			ContainerId containerId = ConverterUtils.toContainerId(envs.get(Environment.CONTAINER_ID.name()));
			appAttemptID = containerId.getApplicationAttemptId();
		}

		if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
			throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV + " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_HOST.name())) {
			throw new RuntimeException(Environment.NM_HOST.name() + " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
			throw new RuntimeException(Environment.NM_HTTP_PORT + " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_PORT.name())) {
			throw new RuntimeException(Environment.NM_PORT.name() + " not set in the environment");
		}
		LOGGER.info("Application master for app" + ", appId=" + appAttemptID.getApplicationId().getId() + ", clustertimestamp=" + appAttemptID.getApplicationId().getClusterTimestamp() + ", attemptId=" + appAttemptID.getAttemptId());
	}

	private void credentials() throws IOException {
		// Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class are marked as LimitedPrivate
		Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
		DataOutputBuffer dob = new DataOutputBuffer();
		credentials.writeTokenStorageToStream(dob);
		// Now remove the AM->RM token so that containers cannot access it.
		Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
		LOGGER.info("Executing with tokens:");
		while (iter.hasNext()) {
			Token<?> token = iter.next();
			LOGGER.info(token);
			if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
				iter.remove();
			}
		}
		@SuppressWarnings("unused")
		ByteBuffer allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

		// Create appSubmitterUgi and add original tokens to it
		String appSubmitterUserName = System.getenv(ApplicationConstants.Environment.USER.name());
		UserGroupInformation appSubmitterUgi = UserGroupInformation.createRemoteUser(appSubmitterUserName);
		appSubmitterUgi.addCredentials(credentials);
	}

	/**
	 * Main run function for the application master
	 *
	 * @throws YarnException
	 * @throws IOException
	 */
	public void run() throws YarnException, IOException {
		LOGGER.info("#启动 ApplicationMaster");
		check_environment();
		credentials();

		NMCallbackHandler nmcallbackhandler = new NMCallbackHandler(this);
		AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler(this, applicationmaster_configuration, nmcallbackhandler);

		amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
		amRMClient.init(conf);
		amRMClient.start();

		nmClientAsync = new NMClientAsyncImpl(nmcallbackhandler);
		nmClientAsync.init(conf);
		nmClientAsync.start();

		RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(NetUtils.getHostname(), -1, "");

		int maxMem = response.getMaximumResourceCapability().getMemory();
		if (applicationmaster_configuration.containerMemory > maxMem) {
			LOGGER.info("#Container memory 配置过多:" + applicationmaster_configuration.containerMemory + ", 使用最大值:" + maxMem);
			applicationmaster_configuration.containerMemory = maxMem;
		}
		int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
		if (applicationmaster_configuration.containerVirtualCores > maxVCores) {
			LOGGER.info("#Container  virtual cores 配置过多:" + applicationmaster_configuration.containerVirtualCores + ", 使用最大值:" + maxVCores);
			applicationmaster_configuration.containerVirtualCores = maxVCores;
		}

		List<Container> previousAMRunningContainers = response.getContainersFromPreviousAttempts();
		LOGGER.info("#appattemptid:" + appAttemptID + " 使用 " + previousAMRunningContainers.size() + " 个已经注册的containers.");
		numAllocatedContainers.addAndGet(previousAMRunningContainers.size());

		int numTotalContainersToRequest = applicationmaster_configuration.numTotalContainers - previousAMRunningContainers.size();
		for (int i = 0; i < numTotalContainersToRequest; ++i) {
			amRMClient.addContainerRequest(setupContainerAskForRM());
		}
		numRequestedContainers.set(applicationmaster_configuration.numTotalContainers);
	}

	protected ContainerRequest setupContainerAskForRM() {
		Priority pri = Priority.newInstance(applicationmaster_configuration.requestPriority);
		Resource capability = Resource.newInstance(applicationmaster_configuration.containerMemory, applicationmaster_configuration.containerVirtualCores);
		ContainerRequest request = new ContainerRequest(capability, null, null, pri);
		return request;
	}

	/**
	 * @param args
	 *            Command line args
	 */
	public static void main(String[] args) {
		ApplicationMaster appMaster = null;
		try {
			appMaster = new ApplicationMaster(new ApplicationMaster_Configuration(args));
			appMaster.run();
		} catch (Throwable t) {
			LOGGER.fatal("#Error running ApplicationMaster", t);
			LogManager.shutdown();
			ExitUtil.terminate(1, t);
		}

		while (appMaster.numCompletedContainers.get() != appMaster.applicationmaster_configuration.numTotalContainers) {
			try {
				Thread.sleep(200);
			} catch (InterruptedException ex) {
				LOGGER.error("", ex);
			}
		}

		appMaster.nmClientAsync.stop();
		try {
			if (appMaster.numFailedContainers.get() == 0) {
				appMaster.amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, null);
				appMaster.amRMClient.stop();
			} else {
				String appMessage = "#Diagnostics." + ", total=" + appMaster.applicationmaster_configuration.numTotalContainers + ", completed=" + appMaster.numCompletedContainers.get() + ", allocated=" + appMaster.numAllocatedContainers.get() + ", failed=" + appMaster.numFailedContainers.get();
				LOGGER.info("#存在错误任务");
				LOGGER.info(appMessage);
				appMaster.amRMClient.unregisterApplicationMaster(FinalApplicationStatus.FAILED, appMessage, null);
				appMaster.amRMClient.stop();
				System.exit(2);
			}
		} catch (YarnException | IOException ex) {
			LOGGER.error("#Failed to unregister application", ex);
		}
		appMaster.amRMClient.stop();

		if (appMaster.numFailedContainers.get() == 0) {
			return;
		} else {
			LogManager.shutdown();
			ExitUtil.terminate(2);
		}
	}
}