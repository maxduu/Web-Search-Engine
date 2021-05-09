package edu.upenn.cis.cis455;

import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.net.httpserver.HttpServer;


public class BonusTuning {


	@Test
	public void tuneWebpage() {
		Webpage page = new Webpage("url", "Kevin Durant - Brooklyn Nets - news and analysis, statistics", "preview", "headers", 1);
		
		Query.bonusWebpage(page, "kevin durant news", new ArrayList<>());
		
		System.out.println(page.getScore());
	}
}
