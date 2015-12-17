package com.infogen.etl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MemoryStats {
	private static final Log LOGGER = LogFactory.getLog(MemoryStats.class);
	private static final long MEGABYTE = 1024L * 1024L;

	public static long bytesToMegabytes(long bytes) {
		return bytes / MEGABYTE;
	}

	public static void main(String[] args) {
		LOGGER.info("a" + null);
	}

	public void printMemoryStats() {

		long freeMemory = bytesToMegabytes(Runtime.getRuntime().freeMemory());
		long totalMemory = bytesToMegabytes(Runtime.getRuntime().totalMemory());
		long maxMemory = bytesToMegabytes(Runtime.getRuntime().maxMemory());

		LOGGER.info("################################################################");
		LOGGER.info("# The amount of free memory in the JVM:      " + freeMemory + "MB");
		LOGGER.info("# The total amount of memory in the JVM:     " + totalMemory + "MB");
		LOGGER.info("# The maximum amount of memory that the JVM: " + maxMemory + "MB");
		LOGGER.info("################################################################");
	}
}
