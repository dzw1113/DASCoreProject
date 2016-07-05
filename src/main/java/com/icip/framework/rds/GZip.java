package com.icip.framework.rds;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;


/**
 * 通过 Apache Tool 进行JAVA tar || tar.gz
 */
public class GZip {
	
	/**
	 * 测试解压归档tar文件
	 */
	public void UnTar(){
		File srcTarFile = new File("F:/wk-storm-dev/DASCore/mysql-bin.000908.tar");
		String destDir = "F:/wk-storm-dev/DASCore/mysql-bin"; 
		boolean boo = false;//是否压缩成功
		try {
			unTar(srcTarFile,destDir);
			boo = true;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}finally{
			//清理操作
			if(!boo)
				deleteDirectory(new File(destDir));//目标文件夹 。清理
		}
	}
	
	public static void main(String[] args) throws Exception {
		GZip jtar = new GZip();
		jtar.UnTar();
	}
	
	/**
	 * 解压tar File
	 * @param file 要解压的tar文件对象
	 * @param outputDir 要解压到某个指定的目录下
	 * @throws IOException
	 */
	public void unTar(File file,String outputDir) throws IOException {
		TarInputStream tarIn = null;
		try{
			tarIn = new TarInputStream(new FileInputStream(file),1024 * 2);
			createDirectory(outputDir,null);//创建输出目录
			TarEntry entry = null;
			while( (entry = tarIn.getNextEntry()) != null ){
				if(entry.isDirectory()){//是目录
					createDirectory(outputDir,entry.getName());//创建空目录
				}else{//是文件
					File tmpFile = new File(outputDir + "/" + entry.getName());
					createDirectory(tmpFile.getParent() + "/",null);//创建输出目录
					OutputStream out = null;
					try{
						out = new FileOutputStream(tmpFile);
						int length = 0;
						byte[] b = new byte[2048];
						while((length = tarIn.read(b)) != -1){
							out.write(b, 0, length);
						}
					}catch(IOException ex){
						throw ex;
					}finally{
						if(out!=null)
							out.close();
					}
				}
			}
			
		}catch(IOException ex){
			throw new IOException("解压归档文件出现异常",ex);
		} finally{
			try{
				if(tarIn != null){
					tarIn.close();
				}
			}catch(IOException ex){
				throw new IOException("关闭tarFile出现异常",ex);
			}
		}
		
	}
	
	
	/**
	 * 构建目录
	 * @param outputDir
	 * @param subDir
	 */
	public void createDirectory(String outputDir,String subDir){
		File file = new File(outputDir);
		if(!(subDir == null || subDir.trim().equals(""))){//子目录不为空
			file = new File(outputDir + "/" + subDir);
		}
		if(!file.exists()){
			file.mkdirs();
		}
	}
	
	/**
	 * 清理文件(目录或文件)
	 * @param file
	 */
	public void deleteDirectory(File file){
		if(file.isFile()){
			file.delete();//清理文件
		}else{
			File list[] = file.listFiles();
			if(list!=null){
				for(File f: list){
					deleteDirectory(f);
				}
				file.delete();//清理目录
			}
		}
	}
}
