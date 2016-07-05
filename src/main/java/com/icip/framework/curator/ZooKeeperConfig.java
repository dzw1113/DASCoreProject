package com.icip.framework.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;

/**
 * 
 * @Description: zk配置
 * @author
 * @date 2016年3月21日 下午4:00:32
 * @update
 */
public class ZooKeeperConfig implements Config {
	@Override
	public byte[] getConfig(String path) throws Exception {
		CuratorFramework client = ZooKeeperFactory.get();
		if (!exists(client, path)) {
			throw new RuntimeException("路径：" + path + "不存在.");
		}
		return client.getData().forPath(path);
	}

	private boolean exists(CuratorFramework client, String path)
			throws Exception {
		Stat stat = client.checkExists().forPath(path);
		return !(stat == null);
	}
}
