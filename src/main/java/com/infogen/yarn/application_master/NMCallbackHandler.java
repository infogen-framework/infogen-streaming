package com.infogen.yarn.application_master;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月3日 下午5:22:57
 * @since 1.0
 * @version 1.0
 */
public class NMCallbackHandler implements NMClientAsync.CallbackHandler {
	private static final Log LOG = LogFactory.getLog(NMCallbackHandler.class);
	private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();
	private final ApplicationMaster AM;

	public NMCallbackHandler(ApplicationMaster applicationMaster) {
		this.AM = applicationMaster;
	}

	public void addContainer(ContainerId containerId, Container container) {
		containers.putIfAbsent(containerId, container);
	}

	@Override
	public void onContainerStopped(ContainerId containerId) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Succeeded to stop Container " + containerId);
		}
		containers.remove(containerId);
	}

	@Override
	public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Container Status: id=" + containerId + ", status=" + containerStatus);
		}
	}

	@Override
	public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Succeeded to start Container " + containerId);
		}
		Container container = containers.get(containerId);
		if (container != null) {
			AM.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
		}
	}

	@Override
	public void onStartContainerError(ContainerId containerId, Throwable t) {
		LOG.error("Failed to start Container " + containerId);
		containers.remove(containerId);
		AM.numCompletedContainers.incrementAndGet();
		AM.numFailedContainers.incrementAndGet();
	}

	@Override
	public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
		LOG.error("Failed to query the status of Container " + containerId);
	}

	@Override
	public void onStopContainerError(ContainerId containerId, Throwable t) {
		LOG.error("Failed to stop Container " + containerId);
		containers.remove(containerId);
	}
}
