package edu.upenn.cis.cis455;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.w3c.dom.NodeList;

import com.google.gson.Gson;

import scala.Tuple2;

import static spark.Spark.*;

public class CreateTitle {
	
	static final String DB_NAME = "postgres";
	static final String USERNAME = "master";
	static final String PASSWORD = "ilovezackives";
	static final int PORT = 5432;
	static final String HOSTNAME = "cis555-project.ckm3s06jrxk1.us-east-1.rds.amazonaws.com";
	static final String CRAWLER_DOCS_TABLE_NAME = "crawler_docs";
	static final String CONTENT_TABLE_NAME = "crawler_content";

    public static void main(String args[]) {
		transform();


    }

	private static void transform() {
		SparkSession spark = SparkSession
				.builder()
				.appName("Query")
				//.master("local[5]")
				.getOrCreate();
		try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


		String jdbcUrl = "jdbc:postgresql://" + HOSTNAME + ":" + PORT + "/" + 
				DB_NAME + "?user=" + USERNAME + "&password=" + PASSWORD;
		Dataset<Row> crawlerDocsDF = spark.read()
				.format("jdbc")
				.option("url", jdbcUrl)
				.option("driver", "org.postgresql.Driver")
				.option("dbtable", CRAWLER_DOCS_TABLE_NAME)
				.load().repartition(500);
		JavaRDD<Row> crawlerDocsRDD = crawlerDocsDF.toJavaRDD();
		crawlerDocsRDD = crawlerDocsRDD.filter(row -> ((String) row.getAs("type")).contains("text/html"));
		JavaPairRDD<Integer, String> idToContent = 
				crawlerDocsRDD.mapToPair(row -> new Tuple2<>(row.getAs("id"), row.getAs("content")));
		JavaPairRDD<Integer, Document> ParsedContent = 
				idToContent.mapToPair(pair -> new Tuple2<>(pair._1, Jsoup.parse(pair._2)));
		JavaPairRDD<Integer, Tuple2<String, Tuple2<String, Tuple2<String,String>>>> addStuff = 
				ParsedContent.mapToPair(pair -> {
					Elements ele = pair._2.getElementsByTag("title");
					String title = "";
					if(!ele.isEmpty()) {
						Element e = ele.get(0);
						 title = e.text();

					}
					String content = "";
					String helpful =  pair._2.select("h1,h2").text();
					ele = pair._2.getElementsByTag("p");
					if(!ele.isEmpty()) {
						content = "";
						int counter = 0;
						while(content.length() < 300 && counter < ele.size()) {
							content += ele.get(counter).text();
							content += " ";
							counter++;
						}
						

					}
					return new Tuple2<>(pair._1, new Tuple2<>(title, new Tuple2<>(content, new Tuple2<>(helpful, pair._2.select("body").text().replaceAll("[^\\x00-\\x7F]", "")
	                        .replaceAll("\u0000", "")))));
				});
		
		JavaRDD<ContentEntry> contents  = addStuff.map(pair -> {
			return new ContentEntry(pair._1, pair._2._1, pair._2._2._1, pair._2._2._2._1, pair._2._2._2._2);
		});
		
		Dataset<Row> contentDF = spark.createDataFrame(contents, ContentEntry.class);
		
		contentDF.write()
		.format("jdbc")
		.option("url", jdbcUrl)
		.option("driver", "org.postgresql.Driver")
		.option("dbtable", CONTENT_TABLE_NAME)
		.option("truncate", true)
		.mode("overwrite")
		.save();
				
	}
}
