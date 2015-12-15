package com.infogen.yarn;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.log4j.LogManager;

import com.infogen.mapper.InfoGen_Mapper;
import com.infogen.yarn.client.Client;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月15日 下午12:54:39
 * @since 1.0
 * @version 1.0
 */
public class InfoGen_Job {
	private static final Log LOGGER = LogFactory.getLog(Client.class);
	private Job_Configuration job_configuration;

	public InfoGen_Job(Job_Configuration job_configuration) {
		this.job_configuration = job_configuration;
	}

	public void setMapper(Class<? extends InfoGen_Mapper> clazz) {
		job_configuration.mapper_clazz = clazz;
	}

	public void run(String app_name, String topic, String zookeeper, Class<? extends InfoGen_Mapper> mapper_clazz) {
		job_configuration.app_name = app_name;
		job_configuration.mapper_clazz = mapper_clazz;
		job_configuration.topic = topic;
		job_configuration.zookeeper = zookeeper;
		// hadoop jar yarn-app-example-0.0.1-SNAPSHOT.jar timo.yarn_app_call_java_daemon.Client
		// -jar yarn-app-example-0.0.1-SNAPSHOT.jar
		// -num_containers 2
		Client client = null;
		ApplicationId appId = null;
		try {
			client = new Client(job_configuration);
			appId = client.run();
		} catch (Throwable t) {
			LOGGER.fatal("#Error running Client", t);
			LogManager.shutdown();
			ExitUtil.terminate(1, t);
		}

		while (true) {
			try {
				Thread.sleep(3 * 1000);
			} catch (InterruptedException e) {
				LOGGER.debug("#Thread sleep in monitoring loop interrupted", e);
			}

			// Get application report for the appId we are interested in
			ApplicationReport report = null;
			try {
				report = client.yarnClient.getApplicationReport(appId);
			} catch (YarnException | IOException e) {
				LOGGER.fatal("#获取 application 状态失败", e);
				LogManager.shutdown();
				ExitUtil.terminate(2, e);
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
				ExitUtil.terminate(2);
			} else {
				LOGGER.fatal("#未知 application 状态");
				LogManager.shutdown();
				ExitUtil.terminate(2);
			}
		}

	}
}
