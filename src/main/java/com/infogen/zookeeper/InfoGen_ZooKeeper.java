package com.infogen.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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

	private static class InnerInstance {
		public static final InfoGen_ZooKeeper instance = new InfoGen_ZooKeeper();
	}

	public static InfoGen_ZooKeeper getInstance() {
		return InnerInstance.instance;
	}

	private ZooKeeper zookeeper;
	private String host_port;
	private Map<String, Set<String>> map_auth_info = new HashMap<>();
	public static final String CONTEXT = "/infogen_consumers";

	public static String topic(String topic) {
		return CONTEXT.concat("/").concat(topic);
	}

	public static String offset(String topic) {
		return CONTEXT.concat("/").concat(topic).concat("/").concat("offset");
	}

	public static String offset(String topic, Integer partition) {
		return CONTEXT.concat("/").concat(topic).concat("/").concat("offset").concat("/").concat(partition.toString());
	}

	public static String partition(String topic) {
		return CONTEXT.concat("/").concat(topic).concat("/").concat("partition");
	}

	public static String partition(String topic, Integer partition) {
		return CONTEXT.concat("/").concat(topic).concat("/").concat("partition").concat("/").concat(partition.toString());
	}

	// 在服务启动时调用
	public void start_zookeeper(String host_port) throws IOException {
		if (zookeeper == null) {
			this.host_port = host_port;
			LOGGER.info("启动zookeeper:".concat(host_port));
			this.zookeeper = new ZooKeeper(host_port, 10000, connect_watcher);

			for (Entry<String, Set<String>> entry : map_auth_info.entrySet()) {
				String scheme = entry.getKey();
				for (String auth : entry.getValue()) {
					zookeeper.addAuthInfo(scheme, auth.getBytes());
				}
			}
			LOGGER.info("启动zookeeper成功:".concat(host_port));
		} else {
			LOGGER.info("已经存在一个运行的zookeeper实例");
		}
	}

	// 只在重启zookeeper时调用
	public void stop_zookeeper() throws InterruptedException {
		LOGGER.info("关闭zookeeper");
		zookeeper.close();
		zookeeper = null;
		LOGGER.info("关闭zookeeper成功");
	}

	public Boolean available() {
		return (zookeeper != null);
	}

	////////////////////////////////////////////////// 安全认证//////////////////////////////////
	public void add_auth_info(String scheme, String auth) {
		Set<String> set_auth = map_auth_info.getOrDefault(scheme, new HashSet<String>());
		if (!set_auth.contains(auth)) {
			set_auth.add(auth);
			zookeeper.addAuthInfo(scheme, auth.getBytes());
		}
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
			LOGGER.info("写入节点数据:".concat(path));
			Stat setData = zookeeper.setData(path, data, version);
			LOGGER.info("写入节点数据成功:".concat(path));
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
					break;
				case Expired:
					try {
						LOGGER.error("zookeeper 连接过期");
						stop_zookeeper();
						start_zookeeper(host_port);
						LOGGER.info(" 重启zookeeper并重新加载所有子节点监听");
					} catch (Exception e) {
						LOGGER.error("zookeeper 重连错误", e);
					}
					break;
				case Disconnected:
					break;
				default:
					break;
				}
			}
		}
	};
}
