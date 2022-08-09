package app;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.HashMap;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import run.SampleApp;

public class CassandraApp {

    public static void writeToDB(SparkConf sparkConf) throws Exception {
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

        StructType movieSchema = new StructType().add("movieId", "long").add("title", "string").add("genre", "string");

        StructType ratingsSchema = new StructType().add("userId", "long").add("movieId", "long").add("rating", "double").add("timestamp", "long");

        InputStream movieCsvStream = SampleApp.class.getClassLoader().getResourceAsStream(SampleApp.INPUT_LOCATION_MOVIE_FILE);
        InputStream ratingsCsvStream = SampleApp.class.getClassLoader().getResourceAsStream(SampleApp.INPUT_LOCATION_MOVIE_RATINGS_FILE);

        File tempFileMovie = File.createTempFile("movies", ".csv");
        File tempFileRatings = File.createTempFile("ratings", ".csv");
        tempFileMovie.deleteOnExit();
        tempFileRatings.deleteOnExit();
        FileOutputStream movieOutputStream = new FileOutputStream(tempFileMovie);
        FileOutputStream ratingsOutputStream = new FileOutputStream(tempFileRatings);
        IOUtils.copy(movieCsvStream, movieOutputStream);
        IOUtils.copy(ratingsCsvStream, ratingsOutputStream);

        Dataset<Row> dfMovies = spark.read().option("mode", "DROPMALFORMED").schema(movieSchema).csv(tempFileMovie.getAbsolutePath());

        dfMovies.show();
        Dataset<Row> dfRatings = spark.read().option("mode", "DROPMALFORMED").schema(ratingsSchema)
                .csv(tempFileRatings.getAbsolutePath()).select("movieId", "rating");

        dfRatings.show();
        Dataset<Row> dfMovieRatings = dfMovies.join(dfRatings, "movieId").select("movieId", "title", "rating");

        Dataset<Row> movieRatingsAgg = dfMovieRatings.groupBy("movieId", "title").avg("rating").withColumnRenamed("avg(rating)", "rating").withColumnRenamed("movieId", "movieid");

        movieRatingsAgg.write().format("org.apache.spark.sql.cassandra").options(new HashMap<String, String>() {
            {
                put("keyspace", "mykeyspace");
                put("table", "movie");
            }
        }).mode(SaveMode.Append).save();
    }

    public static void readFromDB(SparkConf sparkConf) throws Exception {
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        Dataset<Row> cassandraTableData = spark.read().format("org.apache.spark.sql.cassandra").options(new HashMap<String, String>() {
            {
                put("keyspace", "mykeyspace");
                put("table", "movie");
            }
        }).load();

        System.out.println(cassandraTableData.columns());
        cassandraTableData.show();
    }
}
