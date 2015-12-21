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

import com.infogen.util.DefaultEntry;

/**
 * ApplicationMaster用于启动和维护Container
 * 
 * @author larry/larrylv@outlook.com/创建时间 2015年12月21日 下午6:35:33
 * @since 1.0
 * @version 1.0
 */
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

	// 发送请求的数量/准备好的数量/完成的数量/失败的数量
	// Needed as once requested, we should not request for containers again.
	protected final AtomicInteger numRequestedContainers = new AtomicInteger();
	protected final AtomicInteger numAllocatedContainers = new AtomicInteger();
	protected final AtomicInteger numCompletedContainers = new AtomicInteger();
	protected final AtomicInteger numFailedContainers = new AtomicInteger();
	protected final List<Thread> launchThreads = new ArrayList<Thread>();

	public ApplicationMaster(ApplicationMaster_Configuration applicationmaster_configuration) throws ParseException {
		this.applicationmaster_configuration = applicationmaster_configuration;
	}

	/**
	 * @param args
	 *            Command line args
	 */
	public static void main(String[] args) {
		ApplicationMaster appMaster = null;
		DefaultEntry<AMRMClientAsync<ContainerRequest>, NMClientAsync> entry = null;
		try {
			appMaster = new ApplicationMaster(new ApplicationMaster_Configuration(args));
			entry = appMaster.run();
		} catch (Throwable t) {
			LOGGER.fatal("#Error running ApplicationMaster", t);
			LogManager.shutdown();
			ExitUtil.terminate(1, t);
		}

		appMaster.monitoring(entry.getKey(), entry.getValue());
	}

	public ApplicationAttemptId check_environment() {
		// Application Attempt Id ( combination of attemptId and fail count )
		ApplicationAttemptId appAttemptID;
		Map<String, String> envs = System.getenv();
		if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
			throw new IllegalArgumentException("Application Attempt Id not set in the environment");
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
		return appAttemptID;
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
	public DefaultEntry<AMRMClientAsync<ContainerRequest>, NMClientAsync> run() throws YarnException, IOException {
		LOGGER.info("#启动 ApplicationMaster");
		ApplicationAttemptId appAttemptID = check_environment();
		credentials();

		NMCallbackHandler nmcallbackhandler = new NMCallbackHandler(this);
		RMCallbackHandler allocListener = new RMCallbackHandler(this, applicationmaster_configuration, nmcallbackhandler, appAttemptID);

		AMRMClientAsync<ContainerRequest> amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
		amRMClient.init(conf);
		amRMClient.start();

		NMClientAsync nmClientAsync = new NMClientAsyncImpl(nmcallbackhandler);
		nmClientAsync.init(conf);
		nmClientAsync.start();

		allocListener.setAmRMClient(amRMClient);
		allocListener.setNmClientAsync(nmClientAsync);
		nmcallbackhandler.setNmClientAsync(nmClientAsync);

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

		return new DefaultEntry<AMRMClientAsync<ContainerRequest>, NMClientAsync>(amRMClient, nmClientAsync);
	}

	protected ContainerRequest setupContainerAskForRM() {
		Priority pri = Priority.newInstance(applicationmaster_configuration.containerPriority);
		Resource capability = Resource.newInstance(applicationmaster_configuration.containerMemory, applicationmaster_configuration.containerVirtualCores);
		ContainerRequest request = new ContainerRequest(capability, null, null, pri);
		return request;
	}

	private void monitoring(AMRMClientAsync<ContainerRequest> amRMClient, NMClientAsync nmClientAsync) {
		while (numCompletedContainers.get() != applicationmaster_configuration.numTotalContainers) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException ex) {
				LOGGER.error("", ex);
			}
		}

		nmClientAsync.stop();
		try {
			if (numFailedContainers.get() == 0) {
				amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, null);
				amRMClient.stop();
			} else {
				String appMessage = "#Diagnostics." + ", total=" + applicationmaster_configuration.numTotalContainers + ", completed=" + numCompletedContainers.get() + ", allocated=" + numAllocatedContainers.get() + ", failed=" + numFailedContainers.get();
				LOGGER.info("#存在错误任务");
				LOGGER.info(appMessage);
				amRMClient.unregisterApplicationMaster(FinalApplicationStatus.FAILED, appMessage, null);
				amRMClient.stop();
				return;
			}
		} catch (YarnException | IOException ex) {
			LOGGER.error("#Failed to unregister application", ex);
		}
		amRMClient.stop();
	}

}