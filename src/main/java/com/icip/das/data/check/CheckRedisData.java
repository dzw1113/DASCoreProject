package com.icip.das.data.check;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;

import com.icip.das.core.jdbc.DataInitLoader;
import com.icip.das.core.jdbc.DataSourceLoader;
import com.icip.das.core.jdbc.DataSourceUtil;
import com.icip.das.core.redis.RedisClientPool;

public class CheckRedisData {

	private static String schema = "DAS";

	public static void main(String[] args) {
		DataSourceLoader.DATASOURCE_INSTANCE.init();
		DataInitLoader.getTableConfig();
		RedisClientPool.getInstance();

		String tableName = "vt_arreas_info";
		Set<String> set = getRedisKey(tableName);
		gensqlRdKey(tableName, set);
	}

	public static void handleRedisKey(String[] tableNames){
		for(String tableName : tableNames){
			gensqlRdKey(tableName,getRedisKey(tableName));
		}
	}
	
	private static void gensqlRdKey(String tableName, Set<String> set) {
		String insertSql = "insert into TMP_RD_CHECK_KEY(RS_KEY,RS_VAL) values";
		StringBuffer sb = new StringBuffer(insertSql);
		Iterator<String> it = set.iterator();
		int index = 0;
		List<String> list = new ArrayList<String>();
		while (it.hasNext()) {
			String key = it.next();
			int ix = key.indexOf(":");
			String val = key.substring(ix + 1, key.length());
			sb.append("('").append(tableName).append("'").append(",'")
					.append(val).append("'");
			if (index != 0 && (index % 5000 == 0 || index == set.size() - 1)) {
				sb.append(");");
				insertData(sb.toString());
				sb = new StringBuffer(insertSql);
				list = new ArrayList<String>();
			} else {
				sb.append("),");
			}
			list.add(val);
			index++;
		}
	}

	private static void insertData(String insertSql) {
		Connection connection = DataSourceUtil.getConnection(schema);
		Statement stmt = null;
		ResultSet rs = null;

		try {
			stmt = connection.createStatement();
			stmt.executeUpdate(insertSql);

		} catch (Exception e) {
			System.out.println(e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					System.out.println(e);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					System.out.println(e);
				}
			}
			DataSourceUtil.close();
		}
	}

	private static Set<String> getRedisKey(String tableName) {
		Jedis redisPipeClient = RedisClientPool.getResource();
		return redisPipeClient.keys(tableName + ":*");
	}

}
