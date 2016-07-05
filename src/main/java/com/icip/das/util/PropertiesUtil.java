package com.icip.das.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * 工具类：读取properties文件
 * 
 * @author lenovo
 */
public class PropertiesUtil {
	private Properties propertie;
	private InputStream inputFile;
	private FileOutputStream outputFile;

	public static Logger logger = Logger.getLogger(PropertiesUtil.class);

	private static PropertiesUtil instance;

	/**
	 * 初始化Configuration类
	 */
	private PropertiesUtil() {
	}

	public static PropertiesUtil getInstance(String path) { // 对获取实例的方法进行同步
		if (instance == null) {
			synchronized (PropertiesUtil.class) {
				instance = new PropertiesUtil(path);
			}
		}
		return instance;
	}

	/**
	 * 初始化Configuration类
	 * 
	 * @param filePath
	 *            要读取的配置文件的路径+名称
	 */
	private PropertiesUtil(String filePath) {
		propertie = new Properties();
		try {
			inputFile = getClass().getResourceAsStream(filePath);
			propertie.load(inputFile);
			inputFile.close();
		} catch (FileNotFoundException ex) {
			logger.error("读取属性文件--->失败！- 原因：文件路径错误或者文件不存在");
			ex.printStackTrace();
		} catch (IOException ex) {
			logger.error("装载文件--->失败!");
			ex.printStackTrace();
		}
	}

	/**
	 * 重载函数，得到key的值
	 * 
	 * @param key
	 *            取得其值的键
	 * @return key的值
	 */
	public String getValue(String key) {
		if (propertie.containsKey(key)) {
			String value = propertie.getProperty(key);// 得到某一属性的值
			return value;
		} else
			return "";
	}

	/**
	 * 重载函数，得到key的值
	 * 
	 * @param fileName
	 *            properties文件的路径+文件名
	 * @param key
	 *            取得其值的键
	 * @return key的值
	 */
	public String getValue(String fileName, String key) {
		try {
			String value = "";
			inputFile = getClass().getResourceAsStream(fileName);
			propertie.load(inputFile);
			inputFile.close();

			if (propertie.containsKey(key)) {
				value = propertie.getProperty(key);
				return value;
			} else
				return value;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return "";
		} catch (IOException e) {
			e.printStackTrace();
			return "";
		} catch (Exception ex) {
			ex.printStackTrace();
			return "";
		}
	}

	/**
	 * 清除properties文件中所有的key和其值
	 */
	public void clear() {
		propertie.clear();
	}

	/**
	 * 改变或添加一个key的值，当key存在于properties文件中时该key的值被value所代替， 当key不存在时，该key的值是value
	 * 
	 * @param key
	 *            要存入的键
	 * @param value
	 *            要存入的值
	 */
	public void setValue(String key, String value) {
		propertie.setProperty(key, value);
	}

	/**
	 * 将更改后的文件数据存入指定的文件中，该文件可以事先不存在。
	 * 
	 * @param fileName
	 *            文件路径+文件名称
	 * @param description
	 *            对该文件的描述
	 */
	public void saveFile(String fileName, String description) {
		String filePath = getClass().getResource("/").getPath() + fileName;
		File file = new File(filePath);
		try {
			outputFile = new FileOutputStream(file);
			propertie.store(outputFile, description);
			outputFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

}