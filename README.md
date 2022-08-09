# Spark & Cassandra


Description
=======================================
Java application leveraging Apache Spark to join two csv files (movies and ratings)
and flush the result to a Cassandra DB cluster.

Tech Stack
=======================================
- Java 8
- Cassandra DB
- Apache Spark

Steps to run
=======================================
Run the *SampleApp* class


Requirements
=======================================

Cassandra DB
------------------------
   - keyspace   *mykeyspace*
   - table *movie*
       - movieid, title, rating

