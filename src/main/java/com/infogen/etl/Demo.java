package com.infogen.etl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月4日 下午1:07:22
 * @since 1.0
 * @version 1.0
 */
public class Demo {
	private static final Log LOGGER = LogFactory.getLog(Demo.class);

	public static void main(String[] args) throws InterruptedException {
		Demo daemon = new Demo();
		daemon.run();
		LOGGER.info("run------------------------------------------");
	}

	public void run() throws InterruptedException {
		MemoryStats ms = new MemoryStats();
		for (int i = 0; i < 10; i++) {
			Thread.sleep(1000);
			ms.printMemoryStats();
		}
	}
}
