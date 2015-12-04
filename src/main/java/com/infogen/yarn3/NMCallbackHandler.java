package com.infogen.yarn3;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.log4j.Logger;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月1日 下午6:42:31
 * @since 1.0
 * @version 1.0
 */
public class NMCallbackHandler implements NMClientAsync.CallbackHandler {
	private static Logger LOGGER = Logger.getLogger(NMCallbackHandler.class);

	public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
		LOGGER.info("Container Stared " + containerId.toString());
	}

	public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

	}

	public void onContainerStopped(ContainerId containerId) {
		// TODO Auto-generated method stub

	}

	public void onStartContainerError(ContainerId containerId, Throwable t) {
		// TODO Auto-generated method stub

	}

	public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
		// TODO Auto-generated method stub

	}

	public void onStopContainerError(ContainerId containerId, Throwable t) {
		// TODO Auto-generated method stub

	}

}
