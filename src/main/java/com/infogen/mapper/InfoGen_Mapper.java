package com.infogen.mapper;

import java.io.IOException;

import com.infogen.hdfs.InfoGen_OutputFormat;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月15日 上午11:19:33
 * @since 1.0
 * @version 1.0
 */
public interface InfoGen_Mapper {
	public void mapper(String topic, Integer partition, Long offset, String message,InfoGen_OutputFormat output) throws IllegalArgumentException, IOException;
}
