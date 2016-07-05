package com.icip.das.core.exception;

/**
 * 根系统异常码
 */
public interface IRootSysExceptionCodes {

	/** 系统错误：在无法定义具体错误是使用 */
	public final static String SYSTEM_ERROR = "E990000";

	/** 系统错误：超时 */
	public final static String SYSTEM_TIMEOUT = "E990001";

	/** 系统错误：超限 */
	public final static String SYSTEM_LIMITED = "E990002";

	/** 系统错误：线程调度异常 */
	public final static String SYSTEM_THREAD = "E990003";

	/** 系统错误：资源文件异常 */
	public final static String SYSTEM_RESOURCE = "E990004";

	/**系统错误，格式转换异常**/
	public final static String SYSTEM_FORMAT  = "E990005";
	
	/**系统错误，系统中止异常**/
	//public final static String SYSTEM_TERMINATED  = "99000008";
	
}
