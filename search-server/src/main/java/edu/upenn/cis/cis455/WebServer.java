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
import spark.Spark;
import spark.utils.IOUtils;

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
        staticFileLocation("/build");
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
        get("/", (req, res) -> {return IOUtils.toString(Spark.class.getResourceAsStream("index.html"));});

        get("/search", (req, res) -> {
        	return gson.toJson(Query.query(req.queryParams("query"), spark, connect));
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
