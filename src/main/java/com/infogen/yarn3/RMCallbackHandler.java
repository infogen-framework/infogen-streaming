package com.infogen.yarn3;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.log4j.Logger;

public class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
	private static Logger LOGGER = Logger.getLogger(RMCallbackHandler.class);

	private final AtomicInteger sleepSeconds = new AtomicInteger(0);
	private NMClientAsyncImpl amNMClient = null;
	private AtomicInteger numCompletedConatiners = null;
	private Map<ContainerId, Container> runningContainers = new ConcurrentHashMap<ContainerId, Container>();

	public RMCallbackHandler(NMClientAsyncImpl amNMClient, AtomicInteger numCompletedConatiners) {
		this.amNMClient = amNMClient;
		this.numCompletedConatiners = numCompletedConatiners;
	}

	public void onContainersCompleted(List<ContainerStatus> statuses) {
		for (ContainerStatus status : statuses) {
			if (status.getExitStatus() != 0) {
				LOGGER.error("Container error: " + status.getContainerId().toString() + " exitStatus=" + status.getExitStatus());
			} else {
				LOGGER.info("Container Completed: " + status.getContainerId().toString() + " exitStatus=" + status.getExitStatus());
			}
			runningContainers.remove(status.getContainerId());
			numCompletedConatiners.addAndGet(1);
		}
	}

	public void onContainersAllocated(List<Container> containers) {
		for (Container container : containers) {
			LOGGER.info("Container Allocated" + ", id=" + container.getId() + ", containerNode=" + container.getNodeId());
			InfoGen_Yarn_ApplicationMaster.exeService.submit(() -> {
				List<String> commands = new LinkedList<String>();
				commands.add("sleep " + sleepSeconds.addAndGet(1));
				ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(null, null, commands, null, null, null);
				amNMClient.startContainerAsync(container, ctx);
			});
			runningContainers.put(container.getId(), container);
		}
	}

	public void onShutdownRequest() {
	}

	public void onNodesUpdated(List<NodeReport> updatedNodes) {

	}

	public float getProgress() {
		float progress = 0;
		return progress;
	}

	public void onError(Throwable e) {
		LOGGER.error("", e);
	}

}