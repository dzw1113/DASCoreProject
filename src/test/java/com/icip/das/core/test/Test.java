package com.icip.das.core.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 
 * @Description: TODO
 * @author  
 * @date 2016年3月28日 下午6:37:54 
 * @update	
 */
public class Test {

	public static void main(String[] args) {
//		String role = "${price}*${buildArea}";
		String role = "${price}";
//		String role = "${price}*${groundDebrisArea}*${groundDebrisNum}";
		Pattern p = Pattern.compile("\\{.*?\\}");
		Matcher m = p.matcher(role);
		while (m.find()) {
			String param = m.group().replaceAll("\\{\\}", "");
			param = param.substring(1, param.lastIndexOf("}"));
			System.out.println(param);
		}
	}
}
