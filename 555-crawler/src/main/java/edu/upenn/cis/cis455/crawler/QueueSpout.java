package edu.upenn.cis.cis455.crawler;

import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.cis455.crawler.worker.WorkerServer;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;

/**
 * Bolt used to get URLs from the queue
 * @author Kevin Chen
 *
 */
public class QueueSpout implements IRichSpout {
	static Logger log = LogManager.getLogger(QueueSpout.class);
	
    String executorId = UUID.randomUUID().toString();
	SpoutOutputCollector collector;
	
    public QueueSpout() {
    	log.debug("Starting spout");
    }

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url"));	
	}

	@Override
	public void open(Map<String, String> config, TopologyContext topo, SpoutOutputCollector collector) {
        this.collector = collector;	
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {		
		String url = WorkerServer.crawler.queue.take();

		// null means the queue is empty, we don't emit anything
		if (url == null) {
			return;
		}
		
		System.err.println("SPOUT GET: " + url);

		// if we get a url, we emit it to the document fetch bolt
		log.debug(getExecutorId() + " emitting " + url);
		this.collector.emit(new Values<Object>(url));
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}

}
