package com.infogen.mapper;

import java.io.IOException;

import com.infogen.hdfs.InfoGen_OutputFormat;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月15日 上午11:19:33
 * @since 1.0
 * @version 1.0
 */
public abstract class InfoGen_Mapper {
	public abstract void config(final String topic, final Integer partition, String parameters);

	public abstract void mapper(final Long offset, String message, InfoGen_OutputFormat output) throws IllegalArgumentException, IOException;
}
