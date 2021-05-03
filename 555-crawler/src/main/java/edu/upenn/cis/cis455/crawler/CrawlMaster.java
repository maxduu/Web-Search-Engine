package edu.upenn.cis.cis455.crawler;

public interface CrawlMaster {

    /**
     * Workers can poll this to see if they should exit, ie the crawl is processing urls
     */
    public boolean isWorking();

    /**
     * Workers should notify when they are processing an URL
     */
//    public void setWorking(boolean working);

    /**
     * Workers should call this when they exit, so the master knows when it can shut
     * down
     */
    public void notifyThreadExited();
}
