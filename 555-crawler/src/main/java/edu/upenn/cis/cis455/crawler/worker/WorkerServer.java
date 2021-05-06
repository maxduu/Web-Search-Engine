package edu.upenn.cis.cis455.crawler.worker;

import static spark.Spark.*;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.cis455.crawler.Crawler;
import edu.upenn.cis.cis455.storage.WorkerStorage;
import edu.upenn.cis.cis455.storage.WorkerStorageInterface;
import edu.upenn.cis.stormlite.LocalCluster;

public class WorkerServer {

	public static Map<String, String> config;
	public static Crawler crawler;
	public static String masterServer;
	public static WorkerStorageInterface workerStorage;
	private static ExecutorService executor = Executors.newFixedThreadPool(5);
	
	private static boolean stop = false;
	public static Date lastDocumentWrite = new Date();
	
	private static int size;
	private static int count;

	public static void main(String[] args) throws IOException {
		if (args.length < 3) {
			System.out.println("Usage: WorkerServer [port number] [master host/IP]:[master port] [queue directory]");
			System.exit(1);
		}

		int myPort = Integer.valueOf(args[0]);
		masterServer = args[1];
		String storageDirectory = args[2];

		if (!masterServer.startsWith("http")) {
			masterServer = "http://" + masterServer;
		}

		port(myPort);

		File directory = new File(storageDirectory);
		if (!directory.exists()) {
			directory.mkdirs();
		}
		workerStorage = new WorkerStorage(storageDirectory);

		System.out.println("Worker node startup, on port " + myPort);

		post("/start", (req, res) -> {
			final ObjectMapper om = new ObjectMapper();
			om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
			config = om.readValue(req.body(), Map.class);

			size = Integer.parseInt(config.get("size"));
			count = Integer.parseInt(config.get("count"));
			
			crawler = new Crawler(size, count);
			crawler.start();

			return "<h1>Start crawling</h1>";
		});
		
		get("/restart", (req, res) -> {
			crawler.shutdown();
			
			LocalCluster.quit.set(false);
			crawler = new Crawler(size, count);
			crawler.start();
			
			return "<h1>Crawler restarted</h1>";
		});

		post("/enqueue", (req, res) -> {
			if (stop) {
				return "<h1>Worker is shutting down</h1>";
			}
			
			if (req.body() == null) {
				return "<h1>Null request body</h1>";
			}
			
			if (crawler.queue.capacityReached) {
				return "<h1>Queue already has more than enough URLs</h1>";
			}
			
			String body = req.body();

			executor.execute(new Runnable() {
				@Override
				public void run() {
					crawler.queue.put(body);
				}
			});
			return "<h1>URL successfully added to queue</h1>";
		});
		
		get("/alive", (req, res) -> {
			return lastDocumentWrite.toString();
		});

		get("/shutdown", (req, res) -> {
			System.err.println("IN SHUTDOWN");
			
			crawler.queue.pauseQueue();
			stop = true;
			executor.shutdownNow();

			if (crawler != null)
				crawler.queue.pauseQueue();

			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						if (crawler != null)
							crawler.shutdown();
						Thread.sleep(3000);
						workerStorage.close();
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch(Exception e) {}
					System.exit(0);
				}
			}).start();

			return "<h1>Shutdown successful</h1>";
		});

		URL connectUrl = new URL(masterServer + "/workerconnect?port=" + myPort);
		HttpURLConnection conn = (HttpURLConnection) connectUrl.openConnection();
		if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
			throw new RuntimeException("Connect to master failed");
		}
	}

}
