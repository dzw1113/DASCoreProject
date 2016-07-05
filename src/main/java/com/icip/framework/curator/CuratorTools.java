package com.icip.framework.curator;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.framework.constans.DASConstants;

/**
 * 
 * @Description: zookeeper存储配置文件，解析配置文件
 * @author
 * @date 2016年3月22日 上午10:07:45
 * @update
 */
public class CuratorTools implements Serializable {

	private static final long serialVersionUID = 8392569087692959151L;

	private static final Logger LOG = LoggerFactory
			.getLogger(CuratorTools.class);

	private static CuratorFramework zkclient = null;

	static {
		CuratorFramework zclient = ZooKeeperFactory.get();
		zkclient = zclient;
	}

	public static void main(String[] args) throws Exception {
		// 上传REDIS
		 CuratorTools
		 .upload(DASConstants.REDIS_PATH,
		 "F:\\wk-storm-dev\\DASCore\\src\\main\\resources\\redis.properties");
//		 CuratorTools
//		 .upload(DASConstants.KAFKA_PATH,
//		 "F:\\wk-storm-dev\\DASCore\\src\\main\\resources\\kafka.properties");
//		CuratorTools
//				.upload(DASConstants.ZKROOT_PATH,
//						"F:\\wk-storm-dev\\DASCore\\src\\main\\resources\\zookeeper.properties");
		// 读取
		 System.err.println(downCfg(DASConstants.REDIS_PATH));
//		 System.err.println(downCfg(DASConstants.KAFKA_PATH));
		// 获取子节点
		 System.err.println(CuratorTools.getListChildren(DASConstants.ZKROOT_PATH));
		// ct.createrOrUpdate("/zk/cc334/zzz","c");
		// ct.delete("/qinb/bb");
		// ct.checkExist("/zk");
		// ct.read("/jianli/123.txt");
	}

	/**
	 * 
	 * @Description: 创建或更新一个节点
	 * @param @param path 路径
	 * @param @param content 内容
	 * @param @throws Exception
	 * @return void
	 * @throws
	 * @author
	 */
	public static void createrOrUpdate(String path, String content)
			throws Exception {
		zkclient.newNamespaceAwareEnsurePath(path).ensure(
				zkclient.getZookeeperClient());
		zkclient.setData().forPath(path, content.getBytes());
		LOG.info("添加成功!");
	}

	/**
	 * 
	 * @Description: 删除zk节点
	 * @param @param path 路径
	 * @param @throws Exception
	 * @return void
	 * @throws
	 * @author
	 */
	public static void delete(String path) throws Exception {
		zkclient.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
		LOG.info("删除成功!");
	}

	/**
	 * 
	 * @Description: 判断路径是否存在
	 * @param @param path 路径
	 * @param @return
	 * @param @throws Exception
	 * @return boolean
	 * @throws
	 * @author
	 */
	public static boolean checkExist(String path) {
		boolean flag = false;
		try {
			if (zkclient.checkExists().forPath(path) != null) {
				return true;
			}
		} catch (Exception e) {
		}
		return flag;
	}

	/**
	 * 
	 * @Description: 读取的路径
	 * @param @param path 路径
	 * @param @return
	 * @param @throws Exception
	 * @return String
	 * @throws
	 * @author
	 */
	public static String read(String path) throws Exception {
		String data = new String(zkclient.getData().forPath(path), "gbk");
		return data;
	}

	/**
	 * 
	 * @Description: 路径 获取某个节点下的所有子文件
	 * @param @param path
	 * @param @return
	 * @param @throws Exception
	 * @return List<String>
	 * @throws
	 * @author
	 */
	public static List<String> getListChildren(String path) throws Exception {
		List<String> paths = zkclient.getChildren().forPath(path);
		return paths;
	}

	/**
	 * 
	 * @Description: 本地上的文件路径
	 * @param @param zkPath zk上的路径
	 * @param @param localpath 本地文件路径
	 * @param @throws Exception
	 * @return void
	 * @throws
	 * @author
	 */
	public static void upload(String zkPath, String localpath) throws Exception {
		createrOrUpdate(zkPath, "");// 创建路径
		byte[] bs = FileUtils.readFileToByteArray(new File(localpath));
		zkclient.setData().forPath(zkPath, bs);
		LOG.info("上传成功！");
	}

	/**
	 * 
	 * @Description: 下载解析文件
	 * @param @param 路径
	 * @param @return
	 * @param @throws Exception
	 * @return Map<String,Object>
	 * @throws
	 * @author
	 */
	public static Map<String, Object> downCfg(String path) {
		if (!checkExist(path)) {
			LOG.error("该路径不存在！");
			return null;
		} else {
			Map<String, Object> map = new HashMap<String, Object>();
			try {
				String str = read(path);
				String s[] = str.split("\r");
				for (int i = 0; i < s.length; i++) {
					if (s[i].trim().indexOf('#') == -1
							&& !StringUtils.isEmpty(s[i].trim())) {
						Object[] arr = s[i].trim().split("=");
						map.put("" + arr[0], arr[1]);
					}
				}
			} catch (Exception e) {
				LOG.error("读取配置出错！", e);
			}
			return map;
		}

	}

	/**
	 * 
	 * @Description: 下载解析文件
	 * @param @param 路径
	 * @param @return
	 * @param @throws Exception
	 * @return Properties
	 * @throws
	 * @author
	 */
	public static Properties downCfgProperties(String path) {
		Properties pro = new Properties();
		Map<String, Object> map = downCfg(path);
		pro.putAll(map);
		return pro;
	}
}
