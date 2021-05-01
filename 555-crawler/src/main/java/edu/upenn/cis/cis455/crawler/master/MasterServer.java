package edu.upenn.cis.cis455.crawler.master;

import static spark.Spark.*;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.cis455.crawler.utils.DocumentPost;
import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import edu.upenn.cis.cis455.crawler.utils.WorkerRouter;
import edu.upenn.cis.cis455.storage.MasterStorage;
import edu.upenn.cis.cis455.storage.MasterStorageInterface;

public class MasterServer {
	
	private static List<String> workerList = new ArrayList<String>();
	private static MasterStorageInterface masterStorage;
	private static AtomicInteger documentsCrawled = new AtomicInteger();
	private static int stopCount;
	
	private static HttpURLConnection postWorkerStart(String address, Map<String, String> config) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        String configString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(config);
        
        URL url = new URL(address);
		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod("POST");

		OutputStream os = conn.getOutputStream();
		byte[] toSend = configString.getBytes();
		os.write(toSend);
		os.flush();
		
		return conn;
	}
	
	private static void shutdown() throws IOException {
		System.err.println("IN SHUTDOWN");
		// make a request to the workers to shutdown
		for (String worker : workerList) {
			URL url;
			if (worker.startsWith("http")) {
    			url = new URL(worker + "/shutdown");    			
			} else {
    			url = new URL("http://" + worker + "/shutdown");    			
			}

			HttpURLConnection conn = (HttpURLConnection)url.openConnection();
			if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
				throw new RuntimeException("Request failed");
			}
			System.err.println("SHUTDOWN WORKER");
		}
		
		masterStorage.close();

		// Call System.exit via another thread after this function has returned status 200
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.exit(0);
			}
		}).start();
	}
	
	public static void main(String[] args) {
        if (args.length != 5) {
            System.out.println("Usage: Master Server {start URL} {RDS environment path} {max doc size in MB} {number of files to index} {port}");
            System.exit(1);
        }
        
        String startUrl = args[0];
        String storagePath = args[1];
        int size = Integer.valueOf(args[2]);
        stopCount = Integer.valueOf(args[3]);
        int myPort = Integer.valueOf(args[4]);

        port(myPort);
        
        // TODO: THIS IS TEMP UNTIL RDS IS SET UP
        File directory = new File(storagePath);
        if (! directory.exists()){
            directory.mkdirs();
        } 
        masterStorage = new MasterStorage(storagePath);
        
        System.out.println("Master node startup, on port " + myPort);

        get("/workerconnect", (req, res) -> {
        	String workerAddress = "http://" + req.ip() + ":" + req.queryParams("port");
        	System.err.println(workerAddress);
    		if (!workerList.contains(workerAddress)) {
    			workerList.add(workerAddress);
    		}
        	
        	return  "<h1>Received</h1>";
        });
        
		get("/start", (req, res) -> {
			documentsCrawled.set(masterStorage.getCorpusSize());
			
			if (documentsCrawled.get() >= stopCount) {
				shutdown();
				return "<h1>Exceeded document count!</h1>";
			}
			
			Map<String, String> config = new HashMap<String, String>();
			config.put("workers", workerList.toString());
			config.put("size", String.valueOf(size));
			
			for (int i = 0; i < workerList.size(); i++) {
				String dest = workerList.get(i);
				String address = dest + "/start";
				config.put("workerIndex", String.valueOf(i));
				
				if (postWorkerStart(address, config).getResponseCode() != HttpURLConnection.HTTP_OK) {
					throw new RuntimeException("Worker start request failed");
				}
			}
			
			if (WorkerRouter.sendUrlToWorker(startUrl, workerList.toString()).getResponseCode() 
					!= HttpURLConnection.HTTP_OK) {
				throw new RuntimeException("Worker add start URL request failed");
			}

			return "<h1>Started crawling</h1>";
		});
		
		get("/shutdown", (req, res) -> {
			shutdown();
			return "<h1>Shutdown</h1>";
		});
		
		post("/put-content-hash", (req, res) -> {
            String hashedContent = req.body();		
            boolean newHash = masterStorage.addDocumentHash(hashedContent);
            if (!newHash) {
            	halt(409);
            }
            return "";
		});
		
	}	
}
