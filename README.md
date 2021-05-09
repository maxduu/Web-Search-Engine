# README

## Names and SEAS logins
Kevin Chen (kevc528), Maxwell Du (maxdu), Edward Kim (kime022), Andrew Zhao (anzhao)

## Description of Features
### Crawler
The crawler is an extension of the crawl task from HW2. The crawler follows the Mercator model and distributes the tasks involved in the crawl. Added are a worker and master Spark Java servers along with changes to the queue and other aspects of the crawler to increase efficiency.

### Indexer
Indexer that takes documents and created inverted index + create TF/IDF

### PageRank
PageRank algorithm written in SQL that writes to Amazon RDS.

### Query and Search
Query function which returns sorted list of id's matching the query.
Script that can extract title and preview from HTML documents.
Web server API for the query function
UI to display search results.

### Extra Credit
Add Bonuses Here

### Source Files
TODO

## How to Run The Project
### Crawler
The crawler can be run using Maven. Within the `pom.xml` file, there are different executions for the master node and up to 3 worker nodes. To run the master, just simply run `mvn exec:java@master` and to run the workers, run `mvn exec:java@worker[number]`. It is important to note that there are several environment variables that the crawler uses. These are `RDS_USERNAME`, `RDS_PASSWORD`, and `RDS_HOSTNAME`. These must be filled in for the crawler to access and write to the database. Additionally, when using EC2, you will have to change the argument in pom.xml to match what ever location the master node is running on.

### Indexer
Deploy indexer, title script and Pagerank using EMR.

### PageRank

### Query and Search
Every other component can be deployed in EC2.
In particular, we can deploy the web server and the UI on the same EC2 node,
with the web server running on port 45555 and the UI running on port 80.
