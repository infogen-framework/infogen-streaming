package com.infogen.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * zookeeper调用封装
 * 
 * @author larry/larrylv@outlook.com/创建时间 2015年8月3日 上午11:30:44
 * @since 1.0
 * @version 1.0
 */
public class InfoGen_ZooKeeper {
	private static final Logger LOGGER = LogManager.getLogger(InfoGen_ZooKeeper.class.getName());
	public static Boolean alive = true;

	private ZooKeeper zookeeper;

	private String host_port;
	private InfoGen_Zookeeper_Handle_Expired handle;
	public static final String CONTEXT = "/infogen_consumers";

	// /topic
	public static String topic(String topic) {
		return CONTEXT.concat("/").concat(topic);
	}

	// /topic/group
	public static String topic(String topic, String group) {
		return CONTEXT.concat("/").concat(topic).concat("/").concat(group);
	}

	// /topic/group/offset
	public static String offset(String topic, String group) {
		return CONTEXT.concat("/").concat(topic).concat("/").concat(group).concat("/").concat("offset");
	}

	// /topic/group/offset/partition
	public static String offset(String topic, String group, Integer partition) {
		return CONTEXT.concat("/").concat(topic).concat("/").concat(group).concat("/").concat("offset").concat("/").concat(partition.toString());
	}

	// /topic/group/partition
	public static String partition(String topic, String group) {
		return CONTEXT.concat("/").concat(topic).concat("/").concat(group).concat("/").concat("partition");
	}

	// /topic/group/partition/partition
	public static String partition(String topic, String group, Integer partition) {
		return CONTEXT.concat("/").concat(topic).concat("/").concat(group).concat("/").concat("partition").concat("/").concat(partition.toString());
	}

	// 在服务启动时调用
	public void start_zookeeper(String host_port, InfoGen_Zookeeper_Handle_Expired handle) throws IOException {
		if (zookeeper == null) {
			this.host_port = host_port;
			LOGGER.info("启动zookeeper:".concat(host_port));
			this.zookeeper = new ZooKeeper(host_port, 10000, connect_watcher);
			LOGGER.info("启动zookeeper成功:".concat(host_port));
			InfoGen_ZooKeeper.alive = true;
		} else {
			LOGGER.info("已经存在一个运行的zookeeper实例");
		}
	}

	public void restart_zookeeper() throws IOException {
		start_zookeeper(host_port, handle);
	}

	// 只在重启zookeeper时调用
	public void stop_zookeeper() {
		LOGGER.info("关闭zookeeper");
		try {
			if (zookeeper != null) {
				zookeeper.close();
			}
		} catch (InterruptedException e) {
			LOGGER.error("关闭zookeeper异常:", e);
		}
		zookeeper = null;
		LOGGER.info("关闭zookeeper成功");
	}

	public Boolean available() {
		return (zookeeper != null);
	}

	//////////////////////////////////////////////// 节点操作/////////////////////////////////////////////////
	// 只在服务启动时调用,所以采用同步调用,发生异常则退出程序检查
	public String create(String path, byte[] data, List<ACL> acls, CreateMode create_mode) {
		String _return = null;
		try {
			LOGGER.info("创建节点:".concat(path));
			_return = zookeeper.create(path, data, acls, create_mode);
			LOGGER.info("创建节点成功:".concat(_return));
		} catch (KeeperException e) {
			switch (e.code()) {
			case CONNECTIONLOSS:
				LOGGER.warn("连接中断,正在重试创建节点...: " + path);
				create(path, data, create_mode);
				break;
			case NODEEXISTS:
				LOGGER.warn("节点已经存在: " + path);
				_return = Code.NODEEXISTS.name();
				break;
			default:
				LOGGER.error("未知错误: ", KeeperException.create(e.code(), path));
			}
		} catch (Exception e) {
			LOGGER.error("未知程序中断错误: ", e);
		}
		return _return;
	}

	public String create(String path, byte[] data, CreateMode create_mode) {
		return create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, create_mode);
	}

	public Stat exists(String path) {
		Stat exists = null;
		try {
			LOGGER.info("判断节点是否存在:".concat(path));
			exists = zookeeper.exists(path, false);
			LOGGER.info("判断节点是否存在成功:".concat(path));
		} catch (Exception e) {
			LOGGER.error("判断节点是否存在错误: ", e);
		}
		return exists;
	}

	public String create_notexists(String path, CreateMode create_mode) {
		if (exists(path) == null) {
			return create(path, null, create_mode);
		}
		return null;
	}

	public void delete(String path) {
		try {
			LOGGER.info("删除节点:".concat(path));
			zookeeper.delete(path, -1);
			LOGGER.info("删除节点成功:".concat(path));
		} catch (Exception e) {
			LOGGER.error("删除节点错误: ", e);
		}
	}

	public String get_data(String path) {
		try {
			LOGGER.info("获取节点数据:".concat(path));
			byte[] data = zookeeper.getData(path, false, null);
			if (data != null) {
				LOGGER.info("获取节点数据成功:".concat(path));
				return new String(data);
			}
		} catch (Exception e) {
			LOGGER.error("获取节点数据错误: ", e);
		}
		return null;
	}

	public Stat set_data(String path, byte[] data, int version) {
		try {
			LOGGER.debug("写入节点数据:".concat(path));
			Stat setData = zookeeper.setData(path, data, version);
			LOGGER.debug("写入节点数据成功:".concat(path));
			return setData;
		} catch (Exception e) {
			LOGGER.error("写入节点数据失败: ", e);
		}
		return null;
	}

	public List<String> get_childrens(String path) {
		List<String> list = new ArrayList<String>();
		try {
			LOGGER.info("获取子节点目录:".concat(path));
			list = zookeeper.getChildren(path, false);
			LOGGER.info("获取子节点目录成功:".concat(path));
		} catch (Exception e) {
			LOGGER.error("获取子节点目录错误: ", e);
		}
		return list;
	}

	// ///////////////////////////////////////连接 Watcher///////////////////////////////////////////////////
	// 只对 Client 的连接状态变化做出反应
	private Watcher connect_watcher = new Watcher() {
		@Override
		public void process(WatchedEvent event) {
			LOGGER.info("连接事件  path:" + event.getPath() + "  state:" + event.getState().name() + "  type:" + event.getType().name());
			if (event.getType() == Watcher.Event.EventType.None) {
				switch (event.getState()) {
				case SyncConnected:
					InfoGen_ZooKeeper.alive = true;
					break;
				case Expired:
					for (;;) {
						try {
							LOGGER.error("zookeeper 连接过期");
							stop_zookeeper();
							if (handle != null) {
								LOGGER.info("自定义连接过期事件");
								handle.handle_event();
							}
							break;
						} catch (Exception e) {
							LOGGER.error("zookeeper 重连错误", e);
						}

						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							LOGGER.error("", e);
						}
					}
					break;
				case Disconnected:
					InfoGen_ZooKeeper.alive = false;
					break;
				default:
					break;
				}
			}
		}
	};
}
