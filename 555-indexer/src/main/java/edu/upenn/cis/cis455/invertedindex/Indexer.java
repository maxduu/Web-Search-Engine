package edu.upenn.cis.cis455.invertedindex;

import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.tartarus.snowball.ext.englishStemmer;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import scala.Tuple2;

public final class Indexer {
	
	static final String DB_NAME = "postgres";
	static final String USERNAME = System.getenv("RDS_USERNAME");
	static final String PASSWORD = System.getenv("RDS_PASSWORD");
	static final String HOSTNAME = System.getenv("RDS_HOSTNAME");
	static final int PORT = 5432;
	
	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("Usage: crawlerDocsTableName invertedIndexTableName idfsTablename numPartitions");
			System.exit(0);
		}
		
		SparkSession spark = SparkSession
				.builder()
				.appName("Inverted Indexer")
				//.master("local[5]")
				.getOrCreate();
		
		run(spark, args);
		
		spark.close();
	}
	
	private static void run(SparkSession spark, String[] args) {
		
		String crawlerDocsTableName = args[0];
		String invertedIndexTableName = args[1];
		String idfsTableName = args[2];
		int numPartitions = Integer.valueOf(args[3]);
		
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
				.load().repartition(numPartitions);
		
		Set<String> stopWords = new StopWordReader().getStopWords();
		long numDocs = crawlerDocsDF.count();
		
		JavaRDD<Row> crawlerDocsRDD = crawlerDocsDF.toJavaRDD();
		
		JavaPairRDD<Integer, String> idToContent = 
				crawlerDocsRDD.mapToPair(row -> new Tuple2<>(row.getAs("id"), row.getAs("content")));

		// Normalization factor
		double a = .5;
		
		JavaPairRDD<String, Tuple2<Integer, Double>> pairCounts = idToContent.flatMapToPair(pair -> {
			ObjectArrayList<Tuple2<String, Tuple2<Integer, Double>>> tuples = new ObjectArrayList<>();
			int maxCount = 0;
			englishStemmer stemmer = new englishStemmer();
			
			Object2IntOpenHashMap<String> termToCount = new Object2IntOpenHashMap<>();
			
			String content = pair._2;
			Document doc = Jsoup.parse(content);
			String[] allTerms = doc.text().split("[\\p{Punct}\\s]+");
			
			for (String rawTerm : allTerms) {
				String term = rawTerm.toLowerCase()
						.replaceAll("[^\\x00-\\x7F]", "")
						.trim();
				if (!term.isEmpty() && !stopWords.contains(term) && term.length() < 50) {
					stemmer.setCurrent(term);
					if (stemmer.stem()){
					    term = stemmer.getCurrent();
					}
					int count = termToCount.containsKey(term) ? termToCount.getInt(term) + 1 : 1;
					termToCount.put(term, count);
					if (count > maxCount) {
						maxCount = count;
					}
				}
			}
			
			for (String term : termToCount.keySet()) {
				// Normalize term frequency by maximum term frequency in document
				tuples.add(new Tuple2<>(term, new Tuple2<>(pair._1, a + (1 - a) * ((double) termToCount.getInt(term) / maxCount))));
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
		
		System.out.println("Writing to inverted_index");
		
		invertedIndexDF.write()
			.format("jdbc")
			.option("url", jdbcUrl)
			.option("driver", "org.postgresql.Driver")
			.option("dbtable", invertedIndexTableName)
			.option("truncate", true)
			.mode("overwrite")
			.save();
		
		System.out.println("Finished writing to inverted_index");
		
		System.out.println("Writing to idfs");
		
		idfsDF.write()
			.format("jdbc")
			.option("url", jdbcUrl)
			.option("driver", "org.postgresql.Driver")
			.option("dbtable", idfsTableName)
			.option("truncate", true)
			.mode("overwrite")
			.save();
		
		System.out.println("Finished writing to idfs");
		
	}
	
}