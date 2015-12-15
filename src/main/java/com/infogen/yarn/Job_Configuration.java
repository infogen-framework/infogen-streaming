package com.infogen.yarn;

import com.infogen.mapper.InfoGen_Mapper;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月3日 上午11:08:05
 * @since 1.0
 * @version 1.0
 */
public class Job_Configuration {
	public String user = System.getProperty("user.name");
	// 优先级
	public Integer amPriority = 0;
	// 执行队列
	public String amQueue = "default";
	// 内存 for to run the App Master
	public Integer amMemory = 512;
	// cpu 核数 for to run the App Master
	public Integer amVCores = 1;
	// Amt of memory to request for container in which command will be executed
	public Integer containerMemory = 512;
	// Amt. of virtual cores to request for container in which command will be executed
	public Integer containerVirtualCores = 1;
	// No. of containers in which the command needs to be executed
	public Integer numContainers = 1;

	public String nodeLabelExpression = null;

	// flag to indicate whether to keep containers across application attempts.
	public Boolean keepContainers = false;
	public long attemptFailuresValidityInterval = -1;
	// Debug flag
	public Boolean debugFlag = false;

	////////////////////////////////////////////////////////////////////////
	public String app_name = "infogen-etl-kafka";
	public Class<? extends InfoGen_Mapper> mapper_clazz;
	public String topic;
	public String zookeeper;

	public Job_Configuration() {
	}

}
