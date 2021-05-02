package edu.upenn.cis.cis455.invertedindex;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.tartarus.snowball.ext.englishStemmer;

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
	static final String INVERTED_INDEX_TABLE_NAME = "inverted_index_test";
	static final String IDFS_TABLE_NAME = "idfs_test";
	
	public static void main(String[] args) throws Exception {
		
		SparkSession spark = SparkSession
				.builder()
				.appName("Inverted Indexer")
				.master("local[5]")
				.getOrCreate();
		
		run(spark);
		
		spark.close();
	}
	
	private static void run(SparkSession spark) {
		
		String jdbcUrl = "jdbc:postgresql://" + HOSTNAME + ":" + PORT + "/" + 
				DB_NAME + "?user=" + USERNAME + "&password=" + PASSWORD;
		
		Dataset<Row> crawlerDocsDF = spark.read()
				.format("jdbc")
				.option("url", jdbcUrl)
				.option("dbtable", CRAWLER_DOCS_TABLE_NAME)
				.load();
		
		Set<String> stopWords = StopWordReader.getStopWords();
		long numDocs = crawlerDocsDF.count();
		
		JavaRDD<Row> crawlerDocsRDD = crawlerDocsDF.javaRDD();
		
		JavaPairRDD<Integer, String> idToContent = 
				crawlerDocsRDD.mapToPair(row -> new Tuple2<>(row.getAs("id"), row.getAs("content")));

		JavaPairRDD<Tuple2<Integer, String>, Double> pairCounts = idToContent.flatMapToPair(pair -> {
			List<Tuple2<Tuple2<Integer, String>, Double>> tuples = new LinkedList<>();
			Map<String, Integer> termToCount = new HashMap<>();
			// Normalization factor
			double a = .4;
			int maxCount = 0;
			englishStemmer stemmer = new englishStemmer();
			
			String content = pair._2;
			Document doc = Jsoup.parse(content);
			String[] rawTerms = doc.text().split("\\s+");
			
			for (String rawTerm : rawTerms) {
				String term = rawTerm.trim()
						.replaceFirst("^[^a-zA-Z0-9]+", "")
						.replaceAll("[^a-zA-Z0-9]+$", "")
						.toLowerCase();
				if (!stopWords.contains(term)) {
					stemmer.setCurrent(term);
					if (stemmer.stem()){
					    term = stemmer.getCurrent();
					}
					int count = termToCount.containsKey(term) ? termToCount.get(term) + 1 : 1;
					termToCount.put(term, count);
					if (count > maxCount) {
						maxCount = count;
					}
				}
			}
			for (String term : termToCount.keySet()) {
				// Normalize term frequency by maximum term frequency in document
				tuples.add(new Tuple2<>(new Tuple2<>(pair._1, term), a + (1 - a) * ((double) termToCount.get(term) / maxCount)));
			}
			
			return tuples.iterator();
		});

		// Compute IDFs
		JavaRDD<IDFEntry> idfEntries = pairCounts.mapToPair(pair -> new Tuple2<>(pair._1._2, 1))
				.aggregateByKey(0, (v1, x) -> v1 + 1, (v1, v2) -> v1 + v2)
				.map(pair -> {
					IDFEntry entry = new IDFEntry();
					entry.setTerm(pair._1);
					entry.setIdf(Math.log((double) numDocs / (pair._2 + 1)));
					return entry;
				});
		
		// Write to inverted_index table
		JavaRDD<InvertedIndexEntry> invertedIndexEntries = pairCounts.map(pair -> {
			InvertedIndexEntry entry = new InvertedIndexEntry();
			entry.setId(pair._1._1);
			entry.setTerm(pair._1._2);
			entry.setTf(pair._2);
			return entry;
		});
		
		Dataset<Row> invertedIndexDF = spark.createDataFrame(invertedIndexEntries, InvertedIndexEntry.class);
		Dataset<Row> idfsDF = spark.createDataFrame(idfEntries, IDFEntry.class);
		
		System.out.println("Writing to inverted_index");
		
		invertedIndexDF.write()
			.format("jdbc")
			.option("url", jdbcUrl)
			.option("dbtable", INVERTED_INDEX_TABLE_NAME)
			.option("truncate", true)
			.mode("overwrite")
			.save();
		
		System.out.println("Finished writing to inverted_index");
		
		System.out.println("Writing to idfs");
		
		idfsDF.write()
			.format("jdbc")
			.option("url", jdbcUrl)
			.option("dbtable", IDFS_TABLE_NAME)
			.option("truncate", true)
			.mode("overwrite")
			.save();
		
		System.out.println("Finished writing to idfs");

	}
	
}