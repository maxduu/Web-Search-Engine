package edu.upenn.cis.cis455;

import java.io.File;
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
import java.util.Scanner;

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
        Gson gson = new Gson();

        SparkSession spark = SparkSession
				.builder()
				.appName("Query")
				.master("local[5]")
				.getOrCreate();
        port(45555);
        Connection connect = db.getRemoteConnection();
        staticFileLocation("/");
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
        get("/search", (req, res) -> {
        	return gson.toJson(Query.query(req.queryParams("query"), spark, connect));
        });
                
        awaitInitialization();
    }
}
