package edu.upenn.cis.cis455.crawler.worker;

import static spark.Spark.*;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.cis455.crawler.Crawler;
import edu.upenn.cis.cis455.storage.WorkerStorage;
import edu.upenn.cis.cis455.storage.WorkerStorageInterface;

public class WorkerServer {

	public static Map<String, String> config;
	public static Crawler crawler;
	public static String masterServer;
	public static WorkerStorageInterface workerStorage;
	
    public static Map<String, Date> urlSeen = new HashMap<String, Date>();

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
		workerStorage = new WorkerStorage();

		System.out.println("Worker node startup, on port " + myPort);

		post("/start", (req, res) -> {
			final ObjectMapper om = new ObjectMapper();
			om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
			config = om.readValue(req.body(), Map.class);

			crawler = new Crawler(Integer.parseInt(config.get("size")));
			crawler.start();

			return "<h1>Start crawling</h1>";
		});

		post("/enqueue", (req, res) -> {
			crawler.queue.put(req.body());
			return "<h1>URL successfully added to queue</h1>";
		});

		get("/shutdown", (req, res) -> {
			System.err.println("IN SHUTDOWN");

			if (crawler != null)
				crawler.queue.pauseQueue();

			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						while (crawler != null && crawler.isWorking())
							try {
								Thread.sleep(10);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						if (crawler != null)
							crawler.shutdown();
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
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
