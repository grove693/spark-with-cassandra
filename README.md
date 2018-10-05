# spark-with-cassandra


HOW TO RUN APP
=======================================
Simply run it as a simple Java App


Requirements
=======================================

Cassandra DB
------------------------
   - keyspace   mykeyspace
   - table movie
          -movieid, title, rating
          
What does the App do
=================================
 -Reads content from Cassandra
 - Performs a join between two csv files (movies and ratings) and writes the result into cassandra
