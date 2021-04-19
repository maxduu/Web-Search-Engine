package edu.upenn.cis.cis455.crawler.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import com.fasterxml.jackson.databind.ObjectMapper;

public class WorkerRouter {
	
	private static String[] getWorkers(String list) {
		if (list.startsWith("["))
			list = list.substring(1);
		if (list.endsWith("]"))
			list = list.substring(0, list.length() - 1);
		
		String[] servers = list.split(",");
		
		String[] ret = new String[servers.length];
		int i = 0;
		for (String item: servers) {
			item = item.strip();
			if (!item.startsWith("http"))
				ret[i++] = "http://" + item;
			else
				ret[i++] = item;
		}
			
		return ret;
	}
	
	public static HttpURLConnection sendUrlToWorker(String url, String workersString) throws IOException {
		URLInfo urlInfo = new URLInfo(url);
		String[] workers = getWorkers(workersString);
		
		int domainHashIndex = Math.abs(urlInfo.getDomain().hashCode()) % workers.length;
		String shardedWorkerAddress = workers[domainHashIndex];
		
        URL urlObj = new URL(shardedWorkerAddress + "/enqueue");
		HttpURLConnection conn = (HttpURLConnection)urlObj.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod("POST");

		OutputStream os = conn.getOutputStream();
		byte[] toSend = url.getBytes();
		os.write(toSend);
		os.flush();
		
		return conn;
	}
	
	public static HttpURLConnection sendDocumentToMaster(String masterAddress, String url, String contents, 
			String type, boolean modified) throws IOException {
		URL urlObj = new URL(masterAddress + "/putdocument");
		DocumentPost reqBody = new DocumentPost(url, contents, type, modified);
		
		ObjectMapper mapper = new ObjectMapper();
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        String body = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(reqBody);

		HttpURLConnection conn = (HttpURLConnection)urlObj.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod("POST");
		
		OutputStream os = conn.getOutputStream();
		byte[] toSend = body.getBytes();
		os.write(toSend);
		os.flush();
		
		return conn;
	}

}
