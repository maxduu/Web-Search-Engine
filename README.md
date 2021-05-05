# README #

This README would normally document whatever steps are necessary to get your application up and running.

### Names ###
Edward Kim
Maxwell Du
Andrew Zhao
Kevin Chen
### Features ###
Crawler that adds documents to Amazon RDS
Indexer that takes documents and created inverted index + create TF/IDF
PageRank algorithm written in SQL that writes to Amazon RDS.
Query function which returns sorted list of id's matching the query.
Script that can extract title and preview from HTML documents.
Web server API for the query function
UI to display search results. 
### Extra Credit ###
None
### Source Files ###

TODO

### How to Run The Project ###

Deploy indexer, title script and Pagerank using EMR.
Every other component can be deployed in EC2. 
In particular, we can deploy the web server and the UI on the same EC2 node,
with the web server running on port 45555 and the UI running on port 80.