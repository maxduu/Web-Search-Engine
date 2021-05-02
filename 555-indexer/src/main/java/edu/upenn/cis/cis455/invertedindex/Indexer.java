package edu.upenn.cis.cis455.invertedindex;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import scala.Tuple2;

/** 
 * Computes an approximation to pi
 * Usage: JavaSparkPi [slices]
 */
public final class Indexer {
	
	static final String DB_NAME = "postgres";
	static final String USERNAME = "master";
	static final String PASSWORD = "ilovezackives";
	static final int PORT = 5432;
	static final String HOSTNAME = "cis555-project.ckm3s06jrxk1.us-east-1.rds.amazonaws.com";
	
	static final String CRAWLER_DOCS_TABLE_NAME = "crawler_docs_test";
	static final String INVERTED_INDEX_TABLE_NAME = "inverted_index";
	static final String IDFS_TABLE_NAME = "idfs";
	
	public static void main(String[] args) throws Exception {
		
		SparkSession spark = SparkSession
				.builder()
				.appName("Inverted Indexer")
				.master("local[5]")
				.getOrCreate();
		
		String jdbcUrl = "jdbc:postgresql://" + HOSTNAME + ":" + PORT + "/" + 
				DB_NAME + "?user=" + USERNAME + "&password=" + PASSWORD;
		
		Dataset<Row> crawlerDocsDF = spark.read()
				.format("jdbc")
				.option("url", jdbcUrl)
				.option("dbtable", CRAWLER_DOCS_TABLE_NAME)
				.load();
		
		crawlerDocsDF.show(5);
		
		buildInvertedIndex(crawlerDocsDF);
		
		Dataset<Row> invertedIndexDF = spark.read()
				.format("jdbc")
				.option("url", jdbcUrl)
				.option("dbtable", IDFS_TABLE_NAME)
				.load();
		
		Dataset<Row> idfsDF = spark.read()
				.format("jdbc")
				.option("url", jdbcUrl)
				.option("dbtable", IDFS_TABLE_NAME)
				.load();

		// invertedIndexDF.show();
		// idfsDF.show();
	}
	
	private static void buildInvertedIndex(Dataset<Row> crawlerDocsDF) {
		JavaRDD<Row> crawlerDocsRDD = crawlerDocsDF.javaRDD();
		
		JavaPairRDD<Integer, String> idToContent = 
				crawlerDocsRDD.mapToPair(row -> new Tuple2<>(row.getAs("id"), row.getAs("content")));

		JavaPairRDD<Tuple2<Integer, String>, Double> pairCounts = idToContent.flatMapToPair(pair -> {
			List<Tuple2<Tuple2<Integer, String>, Double>> tuples = new LinkedList<>();
			Map<String, Integer> termToCount = new HashMap<>();
			// Normalization factor
			double a = .4;
			int maxCount = 0;
			
			String content = pair._2;
			Document doc = Jsoup.parse(content);
			String[] rawTerms = doc.text().split("\\s+");
			
			for (String rawTerm : rawTerms) {
				String term = rawTerm.toLowerCase();
				// TODO: Stem & ignore if stopword
				int count = termToCount.containsKey(term) ? termToCount.get(term) + 1 : 1;
				termToCount.put(term, count);
				if (count > maxCount) {
					maxCount = count;
				}
			}
			for (String term : termToCount.keySet()) {
				// Normalize term frequency by maximum term frequency in document
				tuples.add(new Tuple2<>(new Tuple2<>(pair._1, term), a + (1 - a) * ((double) termToCount.get(term) / maxCount)));
			}
			
			return tuples.iterator();
		});

		// Compute IDFs
		JavaPairRDD<String, Integer> terms = pairCounts.mapToPair(pair -> new Tuple2<>(pair._1._2, 1));
		JavaPairRDD<String, Integer> termCounts = terms.aggregateByKey(0,
				(v1, i) -> v1 + 1,
				(v1, v2) -> v1 + v2);
		
		
		
		// Write to inverted_index table
		JavaRDD<InvertedIndexEntry> entries = pairCounts.map(pair -> {
			InvertedIndexEntry entry = new InvertedIndexEntry();
			entry.setId(pair._1._1);
			entry.setTerm(pair._1._2);
			entry.setTf(pair._2);
			entry.setWeight(0);
			return entry;
		});
		
		/*
		for (InvertedIndexEntry entry : entries.collect()) {
			System.out.println(entry.getId() + ", " + entry.getTerm() + ", " + entry.getTf() + ", " + entry.getWeight());
		}
		*/
		
		for (Tuple2<String, Integer> tuple : termCounts.collect()) {
			System.out.println(tuple);
		}

	}
	
}