package edu.upenn.cis.cis455;

import java.sql.Connection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.tartarus.snowball.ext.englishStemmer;

import scala.Tuple2;

import com.google.gson.Gson;

public class Query {

	static final String DB_NAME = "postgres";
	static final String USERNAME = "master";
	static final String PASSWORD = "ilovezackives";
	static final int PORT = 5432;
	static final String HOSTNAME = "cis555-project.ckm3s06jrxk1.us-east-1.rds.amazonaws.com";
	
	static final String INVERTED_INDEX_TABLE_NAME = "inverted_index";
	static final String IDFS_TABLE_NAME = "idfs";
	
	static final String URL_TABLE_NAME = "urls";
	static final String CONTENT_TABLE_NAME = "crawler_content";
	
	static final int MAX_RESULTS = 1000;
	
	static final englishStemmer stemmer = new englishStemmer();
	static final Gson gson = new Gson();

	public static String query(String query, SparkSession spark, Connection connect) {
		
		System.out.println("Processing query: " + query);
		
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		String jdbcUrl = "jdbc:postgresql://" + HOSTNAME + ":" + PORT + "/" + 
				DB_NAME + "?user=" + USERNAME + "&password=" + PASSWORD;
		
		String[] inputArgs = query.split(" ");
		
		List<String> fullArgs = new LinkedList<>();
		for (String arg : inputArgs) {
			fullArgs.addAll(Arrays.asList(arg.split("[\\p{Punct}\\s]+")));
		}
		
		// Helper variables to map terms to indices, which we use for our vectors
		int numDistinctSearchTerms = 0;
		Map<String, Integer> termToIndex = new HashMap<>();
		
		// Helper variables to normalize term frequencies for the query vectors
		int maxCount = 0;
		Map<String, Integer> termToCount = new HashMap<>();
		
		// Construct query string for SQL
		String termsString = "(";
		for (String arg : fullArgs) {
			String search = arg.trim()
					.toLowerCase()
					.replaceFirst("^[^a-z0-9]+", "");
			if (!search.isEmpty()) {
				stemmer.setCurrent(search);
				if (stemmer.stem()) {
					search = stemmer.getCurrent();
				}
				termsString += "'" + search + "'" + ",";
				int count = termToCount.containsKey(search) ? termToCount.get(search) + 1 : 1;
				termToCount.put(search, count);
				if (count > maxCount) {
					maxCount = count;
				}
				// Map each distinct term to an index
				if (!termToIndex.containsKey(search)) {
					termToIndex.put(search, numDistinctSearchTerms);
					numDistinctSearchTerms++;
				}
			}
		}
		termsString = termsString.substring(0, termsString.length() - 1) + ")";
		
		Map<String, Double> termToQueryFreq = new HashMap<>();
		for (String key : termToCount.keySet()) {
			termToQueryFreq.put(key, ((double) termToCount.get(key)) / maxCount);
		}
		
		// Perform database queries
		System.out.println("SELECT * FROM " + INVERTED_INDEX_TABLE_NAME + " WHERE term IN " + termsString);
		
		Dataset<Row> invertedIndexDF = spark.read()
				.format("jdbc")
				.option("url", jdbcUrl)
				.option("driver", "org.postgresql.Driver")
				.option("query", "SELECT * FROM " + INVERTED_INDEX_TABLE_NAME + " WHERE term IN " + termsString)
				.load();
		
		System.out.println("SELECT * FROM " + IDFS_TABLE_NAME + " WHERE term IN " + termsString);
		
		Dataset<Row> idfsDF = spark.read()
				.format("jdbc")
				.option("url", jdbcUrl)
				.option("driver", "org.postgresql.Driver")
				.option("query", "SELECT * FROM " + IDFS_TABLE_NAME + " WHERE term IN " + termsString)
				.load();

		JavaRDD<Row> invertedIndexRDD = invertedIndexDF.toJavaRDD();
		JavaRDD<Row> idfsRDD = idfsDF.toJavaRDD();

		// Calculate weights for each query term using IDF
		JavaPairRDD<Integer, Double> queryWeights = idfsRDD.mapToPair(row -> {
			String term = row.getAs("term");
			double weight = .5 + .5 * termToQueryFreq.get(term) * (double) row.getAs("idf");
			return new Tuple2<>(termToIndex.get(term), weight);
		});
		
		// Construct weight vector for query
		final double[] queryVector = new double[numDistinctSearchTerms];
		for (Tuple2<Integer, Double> tup : queryWeights.collect()) {
			queryVector[tup._1.intValue()] = tup._2.doubleValue();
		}
		
		// Group the terms/weights corresponding to each document
		JavaPairRDD<Integer, Iterable<Tuple2<Integer, Double>>> docWeights = 
				invertedIndexRDD
				.mapToPair(row -> new Tuple2<>((int) row.getAs("id"), new Tuple2<>(termToIndex.get(row.getAs("term")), (double) row.getAs("weight"))))
				.groupByKey();

		JavaPairRDD<Integer, Double> sortedDocs = docWeights.mapToPair(pair -> {
			// Construct weight vector for each document
			double[] docVector = new double[queryVector.length];
			for (Tuple2<Integer, Double> tup : pair._2) {
				docVector[tup._1.intValue()] = tup._2.doubleValue();
			}
			return new Tuple2<>(cosineSimilarity(queryVector, docVector), pair._1);
		}).sortByKey(false).mapToPair(pair -> pair.swap());
		
		// Collect the top results
		List<Tuple2<Integer, Double>> sortedDocList = sortedDocs.take(MAX_RESULTS);
		
		/*
		for (Tuple2<Integer, Double> tup : sortedDocList) {
			System.out.println(tup._1 + " " + tup._2);
		}
		*/

		System.out.println("TF/IDF + Pagerank scores finished, using top " + MAX_RESULTS + " documents");
		
		
		/**
		 * Obtain urls, content previews for top ranked documents,
		 * and improve search results with additional measures
		 */
		Map<Integer, Double> ansmap = new HashMap<Integer, Double>();
    	Map<Integer, String> urlmap = new HashMap<Integer, String>();
    	Map<Integer, String[]> contentsmap = new HashMap<Integer, String[]>();
    	Url[] urls = new Url[sortedDocList.size()];
    	int counter = 0;
    	Statement s;
    	
    	StringBuilder queryStringBuilder = new StringBuilder();
    	queryStringBuilder.append("(");
    	
    	for (Tuple2<Integer, Double> tuple : sortedDocList) {
    		int id = tuple._1;
    		ansmap.put(id, tuple._2);
        	queryStringBuilder.append(" " + id + ",");
    	}
    	
    	String queryString = queryStringBuilder.toString();
    	if(queryString.length() > 1) queryString = queryString.substring(0, queryString.length() - 1);
    	queryString += ")";
    	
    	String urlQuery =  String.format("Select * from %s where %s in %s", URL_TABLE_NAME, "id", queryString);
    	String contentQuery = String.format("Select * from %s where %s in %s", CONTENT_TABLE_NAME, "id", queryString);
    	
    	try {
			s = connect.createStatement(0, 0);
			ResultSet rs = s.executeQuery(urlQuery);
			String link = null;
			while (rs.next()) {
				int id = Integer.parseInt(rs.getString(1));
				link = rs.getString(2);
				urlmap.put(id,  link);
		    }
			ResultSet rs2 = s.executeQuery(contentQuery);
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
    	
    	/* BONUSES:
		 * Title contains a search term or stemmed search term
		 * Title contains full search query
		 */
    	double titleTermMatchBonus = 0.1; //0.1
    	double stemmedTitleTermMatchBonus = 0.06; //0.06
    	double titleContainsQueryBonus = 0.3; // 0.3
    	
    	for (int id : ansmap.keySet()) {
    		if (contentsmap.containsKey(id)) {
    			String title = contentsmap.get(id)[0].toLowerCase();
        		Set<String> titleTerms = new HashSet<>(Arrays.asList(title.split("[\\p{Punct}\\s]+")));
        		Set<String> stemmedTitleTerms = new HashSet<>();
        		for (String titleTerm : titleTerms) {
        			stemmer.setCurrent(titleTerm);
    				if (stemmer.stem()) {
    					stemmedTitleTerms.add(stemmer.getCurrent());
    				} else {
    					stemmedTitleTerms.add(titleTerm);
    				}
        		}

        		for (String searchTerm : fullArgs) {
        			if (titleTerms.contains(searchTerm.toLowerCase())) {
        				ansmap.put(id, ansmap.get(id) + titleTermMatchBonus);
        			} else {
        				stemmer.setCurrent(searchTerm);
        				if (stemmer.stem()) {
        					if (stemmedTitleTerms.contains(stemmer.getCurrent())) {
        	    				ansmap.put(id, ansmap.get(id) + stemmedTitleTermMatchBonus);
        	    			}
        				}
        			}
        		}
        		if (title.contains(query.toLowerCase())) {
        			ansmap.put(id, ansmap.get(id) + titleContainsQueryBonus);
        		}
    		}
    	}

        List<Entry<Integer, Double>> list = new LinkedList<Entry<Integer, Double>>(ansmap.entrySet());
        
        // Perform sorting of final scores
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
        	urls[urls.length - 1 - counter] = new Url(url, String.valueOf(e.getValue()), "");
        	counter++;
        }

        System.out.println("Returning final document order.\n");
		return gson.toJson(urls);
	}
	
	/**
	 * @return the cosine similarity between two vectors
	 */
	private static double cosineSimilarity(double[] vectorA, double[] vectorB) {
	    double dotProduct = 0.0;
	    double normA = 0.0;
	    double normB = 0.0;
	    for (int i = 0; i < vectorA.length; i++) {
	        dotProduct += vectorA[i] * vectorB[i];
	        normA += Math.pow(vectorA[i], 2);
	        normB += Math.pow(vectorB[i], 2);
	    }   
	    return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
	}

}
