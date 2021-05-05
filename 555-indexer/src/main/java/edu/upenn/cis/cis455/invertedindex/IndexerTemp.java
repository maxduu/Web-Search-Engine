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

public final class IndexerTemp {
	
	static final String DB_NAME = "postgres";
	static final String USERNAME = "master";
	static final String PASSWORD = "ilovezackives";
	static final int PORT = 5432;
	static final String HOSTNAME = "cis555-project.ckm3s06jrxk1.us-east-1.rds.amazonaws.com";

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.err.println("Please supply a crawler_docs table name.");
			System.exit(0);
		}
		
		SparkSession spark = SparkSession
				.builder()
				.appName("Inverted Indexer")
				.master("local[5]")
				.getOrCreate();
		
		run(args[0], spark);
		
		spark.close();
	}
	
	private static void run(String crawlerDocsTableName, SparkSession spark) {
		
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
				.option("dbtable", crawlerDocsTableName)
				.load().repartition(500);
		
		Set<String> stopWords = new StopWordReader().getStopWords();
		long numDocs = crawlerDocsDF.count();
		
		JavaRDD<Row> crawlerDocsRDD = crawlerDocsDF.toJavaRDD();
		
		JavaPairRDD<Integer, String> idToContent = 
				crawlerDocsRDD.mapToPair(row -> new Tuple2<>(row.getAs("id"), row.getAs("content")));

		JavaPairRDD<String, Tuple2<Integer, Double>> pairCounts = idToContent.flatMapToPair(pair -> {
			List<Tuple2<String, Tuple2<Integer, Double>>> tuples = new LinkedList<>();
			Map<String, Integer> termToCount = new HashMap<>();
			// Normalization factor
			double a = .4;
			int maxCount = 0;
			englishStemmer stemmer = new englishStemmer();
			
			String content = pair._2;
			Document doc = Jsoup.parse(content);
			String[] allTerms = doc.text().split("[\\p{Punct}\\s]+");
			
			for (String rawTerm : allTerms) {
				String term = rawTerm.trim()
						.toLowerCase()
						.replaceFirst("^[^a-z0-9]+", "");
				if (!term.isEmpty() && !stopWords.contains(term)) {
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
			
			// Weight terms in headers to be worth more in TF
			String[] headerTerms = doc.select("h1,h2").text().split("[\\p{Punct}\\s]+");

			for (String rawTerm : headerTerms) {
				String term = rawTerm.trim()
						.toLowerCase()
						.replaceFirst("^[^a-z0-9]+", "");
				if (!term.isEmpty() && !stopWords.contains(term)) {
					// System.out.println("HEADER " + term);
					stemmer.setCurrent(term);
					if (stemmer.stem()){
					    term = stemmer.getCurrent();
					}
					int count = termToCount.containsKey(term) ? termToCount.get(term) + 4 : 4;
					termToCount.put(term, count);
					if (count > maxCount) {
						maxCount = count;
					}
				}
			}
			
			// Weight terms in title to be worth more in TF
			String[] titleTerms = doc.title().split("[\\p{Punct}\\s]+");
			
			System.out.println("title: " + java.util.Arrays.toString(titleTerms));
			
			for (String rawTerm : titleTerms) {
				String term = rawTerm.trim()
						.toLowerCase()
						.replaceFirst("^[^a-z0-9]+", "");
				if (!term.isEmpty() && !stopWords.contains(term)) {
					stemmer.setCurrent(term);
					if (stemmer.stem()){
					    term = stemmer.getCurrent();
					}
					int count = termToCount.containsKey(term) ? termToCount.get(term) + 10 : 10;
					termToCount.put(term, count);
					if (count > maxCount) {
						maxCount = count;
					}
				}
			}
			
			for (String term : termToCount.keySet()) {
				// Normalize term frequency by maximum term frequency in document
				tuples.add(new Tuple2<>(term, new Tuple2<>(pair._1, a + (1 - a) * ((double) termToCount.get(term) / maxCount))));
			}
			
			return tuples.iterator();
		});
		
		JavaPairRDD<String, Double> termToIDF = pairCounts.mapToPair(pair -> new Tuple2<>(pair._1, 1))
				.aggregateByKey(0, (v1, x) -> v1 + 1, (v1, v2) -> v1 + v2)
				.mapToPair(pair -> new Tuple2<>(pair._1, Math.log((double) numDocs / (pair._2 + 1))));
		
		JavaPairRDD<Tuple2<String, Integer>, Double> termToWeights = pairCounts.join(termToIDF)
				.mapToPair(pair -> new Tuple2<>(new Tuple2<>(pair._1, pair._2._1._1), 
						pair._2._1._2 * pair._2._2));
		
		// Construct inverted index entries to add to database
		JavaRDD<InvertedIndexEntry> invertedIndexEntries = termToWeights.map(pair -> {
			InvertedIndexEntry entry = new InvertedIndexEntry();
			entry.setTerm(pair._1._1);
			entry.setId(pair._1._2);
			entry.setWeight(pair._2);
			return entry;
		});
		
		// Construct IDF entries to add to database
		JavaRDD<IDFEntry> idfEntries = termToIDF.map(pair -> {
			IDFEntry entry = new IDFEntry();
			entry.setTerm(pair._1);
			entry.setIdf(pair._2);
			return entry;
		});
		
		Dataset<Row> invertedIndexDF = spark.createDataFrame(invertedIndexEntries, InvertedIndexEntry.class);
		Dataset<Row> idfsDF = spark.createDataFrame(idfEntries, IDFEntry.class);
		
		invertedIndexDF.show(5);
		idfsDF.show(5);
		
	}
	
}