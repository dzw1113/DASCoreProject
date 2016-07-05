package com.icip.das.core.exception;

import java.text.MessageFormat;

/**
 * <pre>
 * 定义 Utility 基础异常以及错误代码
 * 
 *  返回码编码规则：【信息类别(1位)】 + 【系统ID(7位)】 +  【返回码编号(5位]】
 *	信息类别：目前以E/W/I三个字母分别代表如下含义
 *E：Error，表示系统应用错误
 *	W：Warning，表示系统应用警告
 *I：一般提醒信息
 * 
 * 示例1:
 * 
 * 		AppException appException = new AppException("IICIPBS01001");
 * 
 * 
 * 示例2(异常转译（异常链）)
 * 		SQLException sqlException = new SQLException();
 * 		AppException appException = new AppException("IICIPBS01001", sqlException);
 *		
 * </pre>
 */

public class SysException extends RuntimeException implements IRootSysExceptionCodes
{
   /**
	 * Comment for <code>serialVersionUID</code>
	 */
	private static final long serialVersionUID = 6100405588110491907L;


   /** 异常编码	**/
   private String exceptionCode;
   
   private Throwable e;

   
   /**
    * 构造器
    * @param exceptionID 异常代码
    */
   public SysException( String exceptionCode ) 
   {
        this.exceptionCode = exceptionCode ;
   }
   
   /**
    * 构造器
    * @param   exceptionID 异常代码
    * @param   message 异常错误信息
    */
   public SysException(String exceptionID, String message) 
   {
        super( message ) ;
        this.exceptionCode = exceptionID ;
   }
   
	/**
	 * 
	 * @param exceptionId
	 * @param cause
	 */
	public SysException(String exceptionId, Throwable cause ) {
		super(cause);
		this.exceptionCode = exceptionId;
		this.e = cause;
	}
	
	/**
	 * 
	 * @param exceptionCode
	 * @param message
	 * @param cause
	 */
	public SysException(String exceptionCode, String message, Throwable cause) {
		super(message,cause);
		this.exceptionCode = exceptionCode;
		this.e = cause;
	}
	
	/**
	 * <pre>
	 * 提供异常信息拼接的能力
	 * 如：throw new DataServiceNotFoundException("[service={0}] is not existed!", new String[]{serviceName});
	 * </pre>
	 * @param exceptionID 异常编号
	 * @param message 异常原因
	 * @param param 在异常信息中提示的变量数据
	 */
	public SysException(String exceptionID, String message, Object[] param) {
		super(format(message, param));
		this.exceptionCode = exceptionID ;
	}
		

   /**
    * 设置异常标识号
    * @aram code 异常标识号
    */
   public void setExceptionCode( String code ) 
   {
        this.exceptionCode = code ;
   }
   
   /**
    * 取异常标识号
    * @return 异常标识号
    */    
   public String getExceptionCode() 
   {
        return exceptionCode ;
   }
   
   public Throwable getThrowable(){
	   return e;
   }
   
   static String format(String message, Object[] params) {
		if (message.trim().length() > 0) {
			if (params != null && params.length > 0) {
				return new MessageFormat(message).format(params);
			}
		}
		return message;
	}
   
}
