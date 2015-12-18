package com.infogen.etl;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import com.infogen.hdfs.InfoGen_OutputFormat;
import com.infogen.mapper.InfoGen_Mapper;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月10日 下午2:15:29
 * @since 1.0
 * @version 1.0
 */
public class Kafka_To_Hdfs_Mapper implements InfoGen_Mapper {
	// private static Logger LOGGER = Logger.getLogger(Kafka_To_Hdfs_Mapper.class);

	private final ZoneId zoneidPlus8 = ZoneId.of("UTC+8"); // UTC+8

	public void mapper(String topic, Integer partition, Long offset, String message, InfoGen_OutputFormat output) throws IllegalArgumentException, IOException {
		String[] split = message.split(",");
		if (split.length < 9) {
			return;
		}

		LocalDateTime localdatetime = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.valueOf(split[9])), zoneidPlus8);
		StringBuilder dir_stringbuilder = new StringBuilder("hdfs://spark101:8020/infogen/output/").append(topic).append("/");
		dir_stringbuilder.append(localdatetime.getYear()).append("-").append(localdatetime.getMonthValue()).append("-").append(localdatetime.getDayOfMonth()).append("/");
		dir_stringbuilder.append(localdatetime.getHour()).append("/");

		output.write_line(dir_stringbuilder.toString(), message);
	};

}
