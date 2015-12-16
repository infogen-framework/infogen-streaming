/**
 * 
 */
package com.infogen.zookeeper;

/**
 * zookeeper session 失效处理器
 * 
 * @author larry/larrylv@outlook.com/创建时间 2015年8月3日 上午11:30:13
 * @since 1.0
 * @version 1.0
 */
public interface InfoGen_Zookeeper_Handle_Expired {
	public abstract void handle_event() throws Exception;
}
