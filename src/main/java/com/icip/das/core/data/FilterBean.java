package com.icip.das.core.data;

/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年3月10日 下午3:31:32 
 * @update	 
 */
public class FilterBean {
	
	private String logic;
	
	private String regex;
	
	private String regular;
	
	private String range;
	
	private String match;

	public String getLogic() {
		return logic;
	}

	public void setLogic(String logic) {
		this.logic = logic;
	}

	public String getRegex() {
		return regex;
	}

	public void setRegex(String regex) {
		this.regex = regex;
	}

	public String getRegular() {
		return regular;
	}

	public void setRegular(String regular) {
		this.regular = regular;
	}

	public String getMatch() {
		return match;
	}

	public void setMatch(String match) {
		this.match = match;
	}
	
	public String getRange() {
		return range;
	}

	public void setRange(String range) {
		this.range = range;
	}

}
