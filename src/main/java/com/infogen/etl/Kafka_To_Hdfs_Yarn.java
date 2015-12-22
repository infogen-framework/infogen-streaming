package com.infogen.etl;

import org.apache.commons.cli.ParseException;

import com.infogen.yarn.InfoGen_Job;
import com.infogen.yarn.Job_Configuration;

/**
 * Yarn部署示例程序
 * 
 * @author larry/larrylv@outlook.com/创建时间 2015年12月15日 下午2:13:55
 * @since 1.0
 * @version 1.0
 */
public class Kafka_To_Hdfs_Yarn {

	public static void main(String[] args) throws ParseException, ClassNotFoundException {
		Job_Configuration job_configuration = Job_Configuration.get_configuration(args);
		// job_configuration.amMemory = 256;
		// job_configuration.containerMemory = 256;
		// job_configuration.numContainers = 3;
		// job_configuration.zookeeper = "172.16.8.97:2181,172.16.8.98:2181,172.16.8.99:2181";
		// job_configuration.topic = "infogen_topic_tracking";
		// job_configuration.group = "infogen_etl";
		// @SuppressWarnings("unchecked")
		// Class<? extends InfoGen_Mapper> mapper_clazz = (Class<? extends InfoGen_Mapper>) Class.forName("com.infogen.etl.Kafka_To_Hdfs_Mapper");
		// job_configuration.mapper_clazz = mapper_clazz;
		// job_configuration.parameters = "hdfs://spark101:8020/infogen/output/";

		InfoGen_Job infogen_job = new InfoGen_Job(job_configuration, "infogen_etl_kafka_to_hdfs_lzo");
		infogen_job.submit();
	}

}
