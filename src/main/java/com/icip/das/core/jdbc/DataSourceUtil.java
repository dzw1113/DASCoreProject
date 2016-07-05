package com.icip.das.core.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年3月7日 上午9:35:14 
 * @update	
 */
public class DataSourceUtil {

	private static final Object locked = new Object();
	
	private static ThreadLocal<Connection> currentThread = new ThreadLocal<Connection>();  
	
	private static final Logger logger = LoggerFactory.getLogger(DataSourceUtil.class);
	
	/**
	 * 获取数据库连接
	 * @return
	 */
	public static Connection getConnection(String dataSourceName){
		return getConnection(dataSourceName,true);
	}
	
	/**
	 * 获取连接
	 * @param standby 是否线程隔离
	 * @return 数据库连接
	 */
	public static Connection getConnection(String dataSourceName,boolean standby){
		logger.debug("从连接池中获取" + dataSourceName + "连接");
		Connection conn = currentThread.get();
		if (null == conn){
			synchronized (locked) {
				if (null == conn){
					try{
						conn = DataSourceLoader.dataSourceMap.get(dataSourceName).getConnection();
						if (standby)
							currentThread.set(conn);						
					}catch(Exception ex){
						logger.error(ex.toString(), ex);
						throw new RuntimeException(ex);  
					}					
				}
			}
		}		
		return conn;
	}
	
	/**
	 * 开始当前线程事物,关闭自动提交事物能力
	 */
	public static Connection beginTrans(String dataSourceName){
		return beginTrans(dataSourceName,false);
	}
	
	/**
	 * 开始当前线程事物
	 * @param autoCommit 是否自动提交事物
	 */
	public static Connection beginTrans(String dataSourceName,boolean autoCommit){
		
		logger.debug("开启当前线程事务------>");
		
		Connection conn = getConnection(dataSourceName);
		try{
			conn.setAutoCommit(autoCommit);
			return conn;
		}catch(Exception ex){
			throw new RuntimeException(ex);  
		}
	}
	
	/**
	 * 提交当前线程事物
	 */
	public static void commitTrans(String dataSourceName){
		
		logger.debug("提交当前线程事物------->");
		
	    try {  
            Connection conn = currentThread.get();
            if (conn != null) {  
                conn.commit();  
            }  
        } catch (Exception ex) {  
            throw new RuntimeException(ex);  
        }  
	}
	
	/**
	 * 释放当前线程持有的数据库连接
	 */
	public static void close() {
		logger.debug( "释放当前线程持有的数据库连接------>");
        try {  
            Connection conn = currentThread.get();  	           
            if (conn != null) {
                conn.close();  
            }  
        } catch (Exception ex) {  
			logger.error(ex.toString(),ex);
            throw new RuntimeException(ex);  
        } finally {
        	currentThread.remove(); // 千万注意，解除当前线程上绑定的链接（从threadlocal容器中移除对应当前线程的链接）  
        }  
    }
	
	/**
	 * 释放连接池持有的资源
	 */
	public static void shutdown(String dataSourceName){
		logger.debug("释放连接池持有的资源-------->");
		ComboPooledDataSource dataSource = DataSourceLoader.dataSourceMap.get(dataSourceName);
		if (null != dataSource)
			try {
				dataSource.close();
			} catch (SQLException e) {
				logger.error(e.toString(),e);
			}
	}
	
}
