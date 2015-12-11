package com.infogen.kafka;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月11日 上午10:56:28
 * @since 1.0
 * @version 1.0
 */
public interface Message_Handle {
	public void handle(String topic, Integer partition, Long offset, String message);
}
