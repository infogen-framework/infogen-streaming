package com.infogen.yarn3;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Logger;

public class InfoGen_Yarn_ApplicationMaster {
	private static Logger LOGGER = Logger.getLogger(InfoGen_Yarn_Client.class);
	private AMRMClientAsync<ContainerRequest> amRMClient = null;
	private NMClientAsyncImpl amNMClient = null;

	private AtomicInteger numTotalContainers = new AtomicInteger(10);
	private AtomicInteger numCompletedConatiners = new AtomicInteger(0);
	public static final ExecutorService exeService = Executors.newCachedThreadPool();

	private void run() throws YarnException, IOException {
		String containerIdStr = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
		LOGGER.info("containerIdStr " + containerIdStr);
		ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
		ApplicationAttemptId appAttemptId = containerId.getApplicationAttemptId();
		LOGGER.info("appAttemptId " + appAttemptId.toString());

		Configuration conf = new Configuration();

		// 创建 nmClientAsync 与Node Manager 交互
		amNMClient = new NMClientAsyncImpl(new NMCallbackHandler());
		amNMClient.init(conf);
		amNMClient.start();
		// 创建 amRMClient 与Resource Manager 交互
		RMCallbackHandler callbackHandler = new RMCallbackHandler(amNMClient, numCompletedConatiners);
		amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, callbackHandler);
		amRMClient.init(conf);
		amRMClient.start();

		// 3. register with RM and this will heartbeating to RM
		RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(NetUtils.getHostname(), -1, "");
		// 4. Request containers
		response.getContainersFromPreviousAttempts();

		for (int i = 0; i < numTotalContainers.get(); i++) {
			ContainerRequest containerAsk = new ContainerRequest(Resource.newInstance(100, 1), null, null, Priority.newInstance(0)); // 100*10M + 1vcpu
			amRMClient.addContainerRequest(containerAsk);
		}
	}

	void waitComplete() throws YarnException, IOException {
		while (numTotalContainers.get() != numCompletedConatiners.get()) {
			try {
				Thread.sleep(1000);
				LOGGER.info("waitComplete" + ", numTotalContainers=" + numTotalContainers.get() + ", numCompletedConatiners=" + numCompletedConatiners.get());
			} catch (InterruptedException ex) {
				LOGGER.error("", ex);
			}
		}
		LOGGER.info("ShutDown exeService Start");
		exeService.shutdown();
		LOGGER.info("ShutDown exeService Complete");
		amNMClient.stop();
		LOGGER.info("amNMClient  stop  Complete");
		amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "dummy Message", null);
		LOGGER.info("unregisterApplicationMaster  Complete");
		amRMClient.stop();
		LOGGER.info("amRMClient  stop Complete");
	}

	public static void main(String[] args) throws Exception {
		InfoGen_Yarn_ApplicationMaster am = new InfoGen_Yarn_ApplicationMaster();
		am.run();
		am.waitComplete();
	}
}