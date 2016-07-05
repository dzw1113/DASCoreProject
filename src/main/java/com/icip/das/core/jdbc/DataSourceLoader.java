package com.icip.das.core.jdbc;

/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年2月25日 上午11:55:16 
 * @update	
 */
import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;

@SuppressWarnings("unchecked")
public class DataSourceLoader implements java.io.Serializable {

	private static final long serialVersionUID = 4836558885378019045L;

	private static final Logger logger = LoggerFactory.getLogger(DataSourceLoader.class);

	public static Map<String, ComboPooledDataSource> dataSourceMap = new ConcurrentHashMap<String, ComboPooledDataSource>();

	public static final DataSourceLoader DATASOURCE_INSTANCE = new DataSourceLoader();

	public void init() {
		if (null == dataSourceMap || dataSourceMap.isEmpty()) {
			synchronized (this) {
				if (null == dataSourceMap || dataSourceMap.isEmpty()) {
					try {
						load(this.getClass().getClassLoader()
								.getResourceAsStream("DataSourceConfig.xml"));
					} catch (Exception e) {
						logger.info("---------加载出错，采用第二种流形式---------->");
						String path = this.getClass().getResource("/")
								.getPath()
								+ "DataSourceConfig.xml";
						load(path);
					}
				}
			}
		}
	}

	private static void load(String path) {
		File file = new File(path);
		if (null == file || !file.exists()) {
			logger.error( "---------数据源配置不存在---------->" + path);
			return;
		}
		logger.debug("---------启动加载数据源配置 start---------->");
		SAXReader reader = new SAXReader();
		reader.setValidation(false);
		Document document = null;
		try {
			document = reader.read(file);
			Element root = document.getRootElement();
			List<?> parameterList = root.elements();
			if (0 != parameterList.size()) {
				for (int i = 0; i < parameterList.size(); i++) {
					Element element = (Element) parameterList.get(i);

					ComboPooledDataSource dataSource = new ComboPooledDataSource();
					dataSource.setDriverClass(getValue(element, "driverClass"));
					dataSource.setJdbcUrl(getValue(element, "jdbcUrl"));
					dataSource.setUser(getValue(element, "user"));
					dataSource.setPassword(getValue(element, "password"));
					dataSource.getConnection();

					dataSourceMap.put(getValue(element, "sourceName"),
							dataSource);
				}
			}
			logger.debug("---------启动加载数据源配置  end---------->");
		} catch (Exception e) {
			logger.error("数据源出错!");
			logger.error(e.toString(),e);
		}
	}

	private static void load(InputStream is) {
		logger.debug( "---------启动加载数据源配置 start---------->");
		SAXReader reader = new SAXReader();
		reader.setValidation(false);
		Document document = null;
		try {
			document = reader.read(is);
			Element root = document.getRootElement();
			List<?> parameterList = root.elements();
			if (0 != parameterList.size()) {
				for (int i = 0; i < parameterList.size(); i++) {
					Element element = (Element) parameterList.get(i);

					ComboPooledDataSource dataSource = new ComboPooledDataSource();
					dataSource.setDriverClass(getValue(element, "driverClass"));
					dataSource.setJdbcUrl(getValue(element, "jdbcUrl"));
					dataSource.setUser(getValue(element, "user"));
					dataSource.setPassword(getValue(element, "password"));
					if(!StringUtils.isBlank(getValue(element, "maxPoolSize"))){
						dataSource.setMaxPoolSize(Integer.parseInt(getValue(element, "maxPoolSize")));
					}
					if(!StringUtils.isBlank(getValue(element, "minPoolSize"))){
						dataSource.setMinPoolSize(Integer.parseInt(getValue(element, "minPoolSize")));
					}
					if(!StringUtils.isBlank(getValue(element, "maxIdleTime"))){
						dataSource.setMaxIdleTime(Integer.parseInt(getValue(element, "maxIdleTime")));
					}
					if(!StringUtils.isBlank(getValue(element, "acquireIncrement"))){
						dataSource.setAcquireIncrement(Integer.parseInt(getValue(element, "acquireIncrement")));
					}
					if(!StringUtils.isBlank(getValue(element, "idleConnectionTestPeriod"))){
						dataSource.setIdleConnectionTestPeriod(Integer.parseInt(getValue(element, "idleConnectionTestPeriod")));
					}
					
					dataSource.getConnection();

					dataSourceMap.put(getValue(element, "sourceName"),
							dataSource);
				}
			}
			logger.debug("---------启动加载数据源配置  end---------->");
		} catch (Exception e) {
			logger.error( "---------启动加载数据源配置出错！---------->");
			logger.error(e.toString(),e);
		}
	}

	private static String getValue(Element rootElement, String xpath) {
		List<Element> list = rootElement.selectNodes(xpath);
		if (null != list && list.size() > 0) {
			Element element = list.get(0);
			return element.getStringValue();
		}
		return null;
	}

}
