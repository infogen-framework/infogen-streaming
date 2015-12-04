package com.infogen.yarn;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.util.ClassUtil;

import com.infogen.yarn.client.Client;

public class Constants {
	// log4j.properties file
	// if available, add to local resources and set into classpath
	public static final String LOG4J_PATH = "log4j.properties";
	public static final String APP_NAME = "infogen-etl-kafka";
	public static final String LOCAL_JAR_PATH = ClassUtil.findContainingJar(Client.class);
	public static final String JAR_NAME = FilenameUtils.getName(LOCAL_JAR_PATH);
	// Main class to invoke application master
	public static final String APPLICATIONMASTER_CLASS = "com.infogen.yarn.application_master.ApplicationMaster";
	public static final String JAVA_APPLICATION = "com.infogen.etl.Demo";

}
