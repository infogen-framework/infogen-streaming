package com.infogen.etl;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.apache.log4j.Logger;

import com.infogen.hdfs.InfoGen_OutputFormat;
import com.infogen.mapper.InfoGen_Mapper;

/**
 * 
 * @author larry/larrylv@outlook.com/创建时间 2015年12月10日 下午2:15:29
 * @since 1.0
 * @version 1.0
 */
public class Kafka_To_Hdfs_Mapper extends InfoGen_Mapper {
	private static Logger LOGGER = Logger.getLogger(Kafka_To_Hdfs_Mapper.class);
	public String topic;
	public Integer partition;
	public String path_prefix = "";

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.infogen.mapper.InfoGen_Mapper#config(java.lang.String, java.lang.Integer, java.lang.String)
	 */
	@Override
	public void config(String topic, Integer partition, String output, String parameters) {
		this.topic = topic;
		this.partition = partition;
		if (!output.endsWith("/")) {
			output = output.concat("/");
		}
		path_prefix = new StringBuilder(output).append(topic).append("/").toString();
	};

	private final ZoneId zoneidPlus8 = ZoneId.of("UTC+8"); // UTC+8

	public void mapper(Long offset, String key, String message, InfoGen_OutputFormat output) throws IllegalArgumentException, IOException {
		String[] split = message.split(",");
		if (split.length < 9) {
			return;
		}
		LocalDateTime localdatetime = null;
		try {
			localdatetime = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.valueOf(split[9])), zoneidPlus8);
		} catch (Exception e) {
			LOGGER.info(message, e);
			return;
		}
		StringBuilder dir_stringbuilder = new StringBuilder(path_prefix);
		dir_stringbuilder.append(localdatetime.getYear()).append("-").append(localdatetime.getMonthValue()).append("-").append(localdatetime.getDayOfMonth()).append("/");
		dir_stringbuilder.append(localdatetime.getHour());

		output.write_line(dir_stringbuilder.toString(), message);
	}

}
