package edu.upenn.cis.cis455;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.w3c.dom.NodeList;

import com.google.gson.Gson;

import scala.Tuple2;

import static spark.Spark.*;

public class WebServer {
	static final String URL_TABLE_NAME = "urls";
	static final String CONTENT_TABLE_NAME = "crawler_content";
    public static void main(String args[]) {
    	List<String> sids = new ArrayList<String>();
    	List<String> lids = new ArrayList<String>();
        Gson gson = new Gson();

        SparkSession spark = SparkSession
				.builder()
				.appName("Query")
				.master("local[5]")
				.getOrCreate();
        
        port(45555);
        Connection connect = db.getRemoteConnection();
        /*try {
			s = connect.createStatement(0, 0);
			ResultSet rs = s.executeQuery("Select * From crawler_docs_test");
			while (rs.next()) {
		        sids.add(rs.getString(1));
		        lids.add(rs.getString(2));
		    }
        	s.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        finally {
        }
        for (String ss : lids) {
        	System.out.println(ss);
        }*/
        options("/*",
                (request, response) -> {

                    String accessControlRequestHeaders = request
                            .headers("Access-Control-Request-Headers");
                    if (accessControlRequestHeaders != null) {
                        response.header("Access-Control-Allow-Headers",
                                accessControlRequestHeaders);
                    }

                    String accessControlRequestMethod = request
                            .headers("Access-Control-Request-Method");
                    if (accessControlRequestMethod != null) {
                        response.header("Access-Control-Allow-Methods",
                                accessControlRequestMethod);
                    }

                    return "OK";
                });

        before((request, response) -> response.header("Access-Control-Allow-Origin", "*"));

        get("/", (req, res) -> {return "Hello World";});
        get("/search", (req, res) -> {
        	String terms = req.queryParams("query");
        	String[] arg = terms.split(" ");
        	List<Tuple2<Integer, Double>> ans = Query.query(arg, spark);
        	Map<Integer, Double> ansmap = new HashMap<Integer, Double>();
        	Map<Integer, String> urlmap = new HashMap<Integer, String>();
        	Map<Integer, String[]> contentsmap = new HashMap<Integer, String[]>();
        	Url[] urls = new Url[ans.size()];
        	int counter = 0;
        	Statement s;
        	String val = "(";
        	for (Tuple2<Integer, Double> tuple : ans) {
        		ansmap.put(tuple._1, tuple._2);
            	Integer id = tuple._1;
            	val += (" " + id.toString() + ",");
        	}
        	if(val.length() > 1) val = val.substring(0, val.length() - 1);
        	val += ")";
            	String query =  String.format("Select * from %s where %s in %s", URL_TABLE_NAME, "id", val);
            	String query2 = String.format("Select * from %s where %s in %s", CONTENT_TABLE_NAME, "id", val);
            	try {
        			s = connect.createStatement(0, 0);
        			ResultSet rs = s.executeQuery(query);
        			String link = null;
        			while (rs.next()) {
        				Integer id = Integer.parseInt(rs.getString(1));
        				Double d = ansmap.get(id);
        				link = rs.getString(2);
        				urlmap.put(id,  link);
        		    }
        			ResultSet rs2 = s.executeQuery(query2);
        			while(rs2.next()) {
        				String[] add = new String[2];
        				add[0] = rs2.getString(2);
        				add[1] = rs2.getString(3);
        			  contentsmap.put(Integer.parseInt(rs2.getString(1)), add);
        			}
        			 //doc = doc.substring(0, Math.min(1000, doc.length()));
                	s.close();
        		} catch (SQLException e) {
        			// TODO Auto-generated catch block
        			e.printStackTrace();
        		}
            for (int id : ansmap.keySet()) {
            	if (contentsmap.containsKey(id)) {
            		String title = contentsmap.get(id)[0].toLowerCase();
            		for (String searchTerm : arg) {
            			if (title.contains(searchTerm.toLowerCase())) {
            				ansmap.put(id, ansmap.get(id) + .1);
            			}
            		}
            		if (title.contains(terms.toLowerCase())) {
            			ansmap.put(id, ansmap.get(id) + .5);
            		}
            	}
            }
            List<Entry<Integer, Double>> list = new LinkedList<Entry<Integer, Double>>(ansmap.entrySet());
                list.sort(Entry.comparingByValue());
                for (Entry<Integer, Double> e : list) {
                	String url = urlmap.get(e.getKey());
                	String[] stuff = contentsmap.get(e.getKey());
                	/*
                	if(stuff != null) {
                		urls[urls.length - 1 - counter] = new Url(url, stuff[0], stuff[1]);
                	}
                	else {
                    	urls[urls.length - 1 - counter] = new Url(url, "Placeholder Title", "Placeholder content");
                	}*/
                	urls[urls.length - 1 - counter] = new Url(url, "", "");
                	counter++;
                }
        	String ret = gson.toJson(urls);
        	return ret;
        });
        get("/geturl/:id", (req, res) -> {
            Statement s;
           // String json = gson.toJson(listaDePontos);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setValidating(true);
            factory.setIgnoringElementContentWhitespace(true);
            DocumentBuilder builder = factory.newDocumentBuilder();

			String[] result = new String[2];
			String link = null;
			String doc = null;
			String type = null;
        	Integer id = Integer.parseInt(req.params("id"));
        	String query =  String.format("Select * from %s where %s = %s", "urls_test2", id.toString(), "id");
        	String query2 =  String.format("Select * from %s where %s = %s", "crawler_docs_test2", id.toString(), "id");
        	try {
    			s = connect.createStatement(0, 0);
    			ResultSet rs = s.executeQuery(query);
    			while (rs.next()) {
    				link = rs.getString(2);
    		    }
    			 rs = s.executeQuery(query2);
     			while (rs.next()) {
       			 doc = rs.getString(2);
       			 System.out.println(doc);
       			 type = rs.getString(3);
    		    }
    			 //doc = doc.substring(0, Math.min(1000, doc.length()));
            	s.close();
    		} catch (SQLException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
            finally {
            }        	
        	Document d =  Jsoup.parse(doc);
        	
        	Elements node = d.getElementsByTag("title");;
        	String title = node.get(0).text();
        	Entry e = null;
        	String ret = gson.toJson(e);
        	return ret;
        });
        awaitInitialization();
    }
}
