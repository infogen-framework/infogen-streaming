package com.infogen.hdfs;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月15日 上午11:14:30
 * @since 1.0
 * @version 1.0
 */
public interface InfoGen_OutputFormat {
	public void write_line(String path, String topic, Integer partition, Long offset, String message);

	public Boolean close_all();

}
