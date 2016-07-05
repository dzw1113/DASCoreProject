package com.icip.framework.rds;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesRequest;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse.BinLogFile;

public class BinlogDescribeBackupPolicy {
	private static Logger log = LoggerFactory
			.getLogger(BinlogDescribeBackupPolicy.class);

	public static void main(String[] args) throws ClientException {
		DescribeBinlogFilesRequest request = new DescribeBinlogFilesRequest();
		request.setDBInstanceId("rds1l11m40sy648fh9nc");
		// 查询开始时间，格式：yyyy-MM-dd’T’HH:mm:ssZ
		request.setActionName("DescribeBinlogFiles");
		request.setStartTime("2015-06-11T15:00:00Z");
		request.setEndTime("2016-05-24T11:00:00Z");
		request.setPageSize(100);
		request.setPageNumber(1);

		IClientProfile profile = DefaultProfile.getProfile("cn-shenzhen",
				"bLRnRTdnA984nRua", "eqiKFN5RkQ2hUOuerN8HW43eV3adl7");
		IAcsClient client = new DefaultAcsClient(profile);

		DescribeBinlogFilesResponse response = client.getAcsResponse(request);
		System.err.println(response.getTotalRecordCount());

		addDownTask(response.getItems());

	}

	/**
	 * @Description: 多线程下载
	 * @param @param flist
	 * @return void
	 * @throws
	 * @author
	 */
	public static void addDownTask(List<BinLogFile> flist) {
		ExecutorService es = Executors.newFixedThreadPool(8);
		for (int i = 0; i < flist.size(); i++) {
			BinLogFile binLogFile = flist.get(i);
			es.execute(new DownLoadFile(binLogFile.getDownloadLink(), null));
			System.err.println(binLogFile.getDownloadLink());
			log.info("加入下载任务！"+i);
		}
		es.shutdown();
	}

}

class DownLoadFile extends Thread {

	private Logger log = LoggerFactory.getLogger(DownLoadFile.class);
	private String localPath;
	private String url;

	public DownLoadFile(String url, String localPath) {
		this.url = url;
		this.localPath = localPath;
	}

	@Override
	public void run() {
		InputStream in;
		try {
			if (localPath == null) {
				localPath = url.substring(url.lastIndexOf("/") + 1,
						url.indexOf("?"));
			}
			in = new URL(url).openStream();
			byte[] gif = IOUtils.toByteArray(in);
			FileUtils.writeByteArrayToFile(new File(localPath), gif);
			IOUtils.closeQuietly(in);
			log.info("下载文件" + localPath);
		} catch (MalformedURLException e) {
			log.error("无效地址", e);
		} catch (IOException e) {
			log.error("文件读写出错", e);
		}

	}
}
