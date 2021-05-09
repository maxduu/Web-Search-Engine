package edu.upenn.cis.cis455;

import java.util.ArrayList;
import java.util.HashMap;
import org.junit.Test;



public class BonusTuning {


	@Test
	public void tuneWebpage() {
		Webpage page = new Webpage("url", "Kevin Durant - Brooklyn Nets - news and analysis, statistics", "preview", "headers", 1);
		
		Query.bonusWebpage(page, "kevin durant news", new ArrayList<>(), new HashMap<>());
		
		System.out.println(page.getScore());
	}
}
